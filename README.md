# Tesla Sentiment and Stock-Price Analytics (dbt)

This dbt project transforms a daily Tesla dataset into analytics-ready features, a simple price prediction, and row-level prediction-error reporting. It is designed for Snowflake and follows a compact ELT pattern: ingest data upstream into a raw table, transform it with dbt, and retain historical source versions with a snapshot.

> **Important:** `model__predict_price` is a rule-based scoring model, not a trained or production-grade machine-learning model. Its coefficients combine inputs with different units and should be treated as a demonstration baseline.

## Architecture

```text
Snowflake raw.tsla_sentiment_merged
             |
             +--> snapshot: snapshots.stock_data_snapshot
             |
             +--> price features -----------+
             |                              |
             +--> sentiment features -------+--> combined features
                                                |
                                                +--> predicted price
                                                       |
                                                       +--> prediction evaluation
```

| Layer | dbt schema | Materialization | Purpose |
|---|---|---|---|
| Source | `raw` | External to dbt | Daily Tesla OHLCV and sentiment inputs. |
| Input / staging | `staging` | Views | Derives reusable price and sentiment features. |
| Output / analytics | `analytics` | Tables | Produces predictions and error metrics for consumption. |
| Snapshot | `snapshots` | dbt snapshot table | Preserves version history of the raw source by `date`. |

The database for both the declared source and snapshot target is currently set to `dev`. This is a project convention hard-coded in `models/sources.yml` and `snapshots/stock_data_snapshot.sql`; align it with your deployment database before promoting beyond development.

## Source contract

dbt expects this Snowflake table to exist before any models run:

```text
dev.raw.tsla_sentiment_merged
```

| Column | Used for |
|---|---|
| `date` | Business key, ordering key, and snapshot `unique_key`. |
| `open`, `close`, `high`, `low`, `volume` | Daily market-price and volume features. |
| `sentiment_score`, `weighted_sentiment` | Daily sentiment signals. |
| `tweet_count`, `total_likes` | Social-engagement signals. |

Operationally, the upstream load should provide one row per `date`, use a consistently typed date field, and load complete daily records. The current models do not deduplicate source rows; duplicates would multiply records in the joined feature model and compromise window calculations.

## Data models and lineage

| Model | Depends on | Output / transformation |
|---|---|---|
| `feature_engineering__price_features` | `raw.tsla_sentiment_merged` | Renames OHLC fields; calculates intraday percent change, high-low range, and a 7-day rolling close average. |
| `feature_engineering__sentiment_features` | `raw.tsla_sentiment_merged` | Selects sentiment/engagement measures; calculates prior-day sentiment and a 3-day rolling sentiment average. |
| `feature_engineering__combined_features` | Both feature models | Inner-joins features on `date`; adds `weighted_tweet_impact`. |
| `model__predict_price` | Combined features | Calculates `predicted_close_price` from fixed weighted inputs. |
| `evaluation__prediction_output` | Prediction model | Publishes actual versus predicted close, absolute/relative error, and volume. |
| `stock_data_snapshot` | `raw.tsla_sentiment_merged` | Uses the `check` strategy with all columns to record changes to each date. |

## Feature definitions

| Field | Definition |
|---|---|
| `price_change_pct` | `(close - open) / open * 100`; `NULL` when `open` is zero. |
| `high_low_range_pct` | `(high - low) / open * 100`; `NULL` when `open` is zero. |
| `avg_7d_close` | Average `close` across the current row and six preceding dates. |
| `lag_1_sentiment` | Previous row's `sentiment_score`, ordered by date. |
| `avg_3d_sentiment` | Average `sentiment_score` across the current row and two preceding dates. |
| `weighted_tweet_impact` | `weighted_sentiment * tweet_count`. |
| `predicted_close_price` | `0.5 * sentiment_score + 0.2 * weighted_sentiment + 0.05 * tweet_count + 0.25 * avg_7d_close`. |
| `prediction_error` | Absolute difference between actual and predicted close. |
| `relative_error` | Absolute error divided by actual close. |

Window calculations use physical preceding rows (`ROWS BETWEEN ...`) rather than calendar days. Missing trading dates are therefore expected to be skipped naturally, but duplicate dates must be prevented upstream.

## Repository layout

```text
models/
  sources.yml                         # Raw-source declaration
  input/                              # Feature-engineering views
  output/                             # Analytics tables
snapshots/stock_data_snapshot.sql     # Historical raw-data capture
dbt_project.yml                       # Project paths and model materializations
profiles.yml                          # Snowflake profile driven by environment variables
```

`analyses`, `macros`, `seeds`, and `tests` are present but currently contain no active project assets.

## Setup

1. Install a compatible dbt Snowflake adapter (for example, `dbt-snowflake`) and make the `dbt` command available on your PATH.
2. Set the connection variables consumed by `profiles.yml`:

   ```powershell
   $env:DBT_ACCOUNT = "<snowflake_account>"
   $env:DBT_DATABASE = "dev"
   $env:DBT_USER = "<user>"
   $env:DBT_PASSWORD = "<password_or_secret>"
   $env:DBT_ROLE = "<role>"
   $env:DBT_WAREHOUSE = "<warehouse>"
   $env:DBT_TYPE = "snowflake"
   ```

3. Ensure the target Snowflake role can read `dev.raw.tsla_sentiment_merged`, create views in `staging`, create tables in `analytics`, and create snapshots in `snapshots`.

Credentials are intentionally supplied through environment variables. Do not commit secrets or a populated `.env` file.

## Runbook

Validate connectivity and project configuration:

```powershell
dbt debug
dbt parse
```

Build the current-state transformation pipeline:

```powershell
dbt run
```

Capture historical raw-source changes:

```powershell
dbt snapshot
```

Run checks and generate documentation when tests are added:

```powershell
dbt test
dbt docs generate
dbt docs serve
```

For a normal scheduled load, complete the upstream raw-table load first, run `dbt snapshot`, then run the models and tests. Execute snapshots separately from `dbt run`; snapshots are not models.

## Data-quality and production recommendations

The project currently declares no dbt tests. Before operational use, add schema tests for source columns and at least these assertions:

- `date` is `not_null` and `unique` in the raw daily input.
- `open`, `close`, `high`, `low`, and `volume` are present and within valid ranges.
- Sentiment and engagement columns are present or have an explicit null-handling policy.
- The combined model has one row per date and has expected freshness.

Also consider replacing the fixed prediction expression with a versioned trained-model inference process, documenting feature scaling, and guarding `relative_error` with `NULLIF(close_price, 0)` to avoid division by zero.

## Current limitations

- No source freshness checks, schema tests, or model descriptions are implemented.
- The feature join is an inner join, so any date missing from either feature stream is excluded.
- Snapshot change detection checks every column, which is thorough but can increase processing cost as the source widens.
- The project uses one Snowflake thread and hard-coded `dev` source/snapshot database references.


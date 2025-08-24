from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import yfinance as yf

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def load_stock_data_to_snowflake(target_table: str, start_year=2023):
    cur = return_snowflake_conn()
    try:
        start_date = f"{start_year}-01-01"
        tsla = yf.Ticker("TSLA")
        data = tsla.history(start=start_date, auto_adjust=False)

        data.reset_index(inplace=True)
        data['Date'] = pd.to_datetime(data['Date']).dt.date
        stock_df = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                date DATE PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT
            );
        """)
        cur.execute(f"DELETE FROM {target_table};")

        for _, r in stock_df.iterrows():
            cur.execute(f"""
                INSERT INTO {target_table}
                (date, open, high, low, close, volume)
                VALUES ('{r['Date']}', {r['Open']}, {r['High']}, {r['Low']}, {r['Close']}, {r['Volume']});
            """)
        cur.execute("COMMIT;")
        print(f"✅ Stock data (via Ticker.history) loaded into {target_table}.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()

@task
def load_sentiment_data_to_snowflake(target_table: str, start_year=2023):
    cur = return_snowflake_conn()
    try:
        df = pd.read_csv('/opt/airflow/data/daily_sentiment.csv')
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= start_year]
        df['date'] = df['date'].dt.date

        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                date DATE PRIMARY KEY,
                sentiment_score FLOAT,
                weighted_sentiment FLOAT,
                tweet_count INT,
                total_likes INT
            );
        """)
        cur.execute(f"DELETE FROM {target_table};")

        for _, r in df.iterrows():
            cur.execute(f"""
                INSERT INTO {target_table}
                (date, sentiment_score, weighted_sentiment, tweet_count, total_likes)
                VALUES ('{r['date']}', {r['sentiment_score']}, {r['weighted_sentiment']},
                        {r['tweet_count']}, {r['total_likes']});
            """)
        cur.execute("COMMIT;")
        print(f"✅ Sentiment data loaded into {target_table}.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()

@task
def merge_stock_and_sentiment_to_raw(raw_table: str):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {raw_table} (
                date DATE PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                sentiment_score FLOAT,
                weighted_sentiment FLOAT,
                tweet_count INT,
                total_likes INT,
                price_change_pct FLOAT
            );
        """)

        cur.execute(f"""
            MERGE INTO {raw_table} AS target
            USING (
                SELECT
                    stock.date,
                    stock.open,
                    stock.high,
                    stock.low,
                    stock.close,
                    stock.volume,
                    sentiment.sentiment_score,
                    sentiment.weighted_sentiment,
                    sentiment.tweet_count,
                    sentiment.total_likes,
                    ((stock.close - stock.open) / stock.open) * 100 AS price_change_pct
                FROM
                    dev.raw.tesla_stock_data AS stock
                LEFT JOIN
                    dev.raw.tesla_tweet_data AS sentiment
                ON stock.date = sentiment.date
            ) AS source
            ON target.date = source.date
            WHEN MATCHED THEN UPDATE SET
                open = source.open,
                high = source.high,
                low = source.low,
                close = source.close,
                volume = source.volume,
                sentiment_score = source.sentiment_score,
                weighted_sentiment = source.weighted_sentiment,
                tweet_count = source.tweet_count,
                total_likes = source.total_likes,
                price_change_pct = source.price_change_pct
            WHEN NOT MATCHED THEN INSERT (
                date, open, high, low, close, volume,
                sentiment_score, weighted_sentiment,
                tweet_count, total_likes, price_change_pct
            )
            VALUES (
                source.date, source.open, source.high, source.low, source.close, source.volume,
                source.sentiment_score, source.weighted_sentiment,
                source.tweet_count, source.total_likes, source.price_change_pct
            );
        """)
        cur.execute("COMMIT;")
        print(f"✅ Incrementally merged into {raw_table}.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()

with DAG(
    dag_id='tesla_stock_sentiment_final_etl',
    start_date=datetime(2025, 2, 21),
    catchup=False,
    schedule='30 2 * * *',  # Daily at 2:30 AM
    tags=['ETL', 'tesla', 'sentiment', 'snowflake']
) as dag:

    target_table = "dev.raw.tesla_stock_data"
    target_table_sentiment = "dev.raw.tesla_tweet_data"
    raw_table = "dev.raw.tsla_sentiment_merged"

    stock_task = load_stock_data_to_snowflake(target_table)
    sentiment_task = load_sentiment_data_to_snowflake(target_table_sentiment)
    merge_task = merge_stock_and_sentiment_to_raw(raw_table)

    [stock_task, sentiment_task] >> merge_task

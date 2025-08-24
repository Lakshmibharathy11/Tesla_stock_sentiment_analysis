WITH raw_data AS (
    SELECT *
    FROM {{ ref('your_raw_table') }}
),

features AS (
    SELECT
        date,
        sentiment_score,
        weighted_sentiment,
        total_likes,
        tweet_count,
        open_price,
        close_price,
        volume,
        LAG(sentiment_score, 1) OVER (ORDER BY date) AS lag_1_sentiment,
        AVG(sentiment_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_3d_sentiment,
        AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d_close,
        weighted_sentiment * tweet_count AS weighted_tweet_impact
    FROM raw_data
),

predictions AS (
    SELECT
        *,
        PREDICT(tesla_price_predictor, 
            sentiment_score,
            weighted_sentiment,
            total_likes,
            tweet_count,
            lag_1_sentiment,
            avg_3d_sentiment,
            avg_7d_close,
            weighted_tweet_impact
        ) AS predicted_close_price
    FROM features
)

SELECT * 
FROM predictions

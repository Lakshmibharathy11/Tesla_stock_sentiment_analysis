WITH prediction_data AS (
    SELECT *
    FROM {{ ref('input') }}
)

SELECT
    date,
    sentiment_score,
    weighted_sentiment,
    total_likes,
    tweet_count,
    close_price AS actual_close_price,
    predicted_close_price,
    ABS(close_price - predicted_close_price) AS prediction_error,
    open_price,
    volume
FROM prediction_data

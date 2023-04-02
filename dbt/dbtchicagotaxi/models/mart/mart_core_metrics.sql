-- purpose: mart table containing key metrics by day with some basic dimensions for standard reporting

{{
  config(
    materialized = 'table',
    )
}}

WITH fct_taxi_trips AS(
    SELECT * FROM {{ ref('fct_taxi_trips') }}
),
dim_dates AS(
    SELECT * FROM {{ ref('dimdates') }}
),
weather AS (
    SELECT * FROM {{ ref('chicago_weather_history') }}
),


-- note: move this CTE into a separate ephemeral model for re-use
agg_daily_trips AS(
    SELECT
     trip_date AS date,

     payment_type,

     COUNT(tt.taxi_id) AS trip_count,
     SUM(tt.trip_distance_km) AS total_trip_distance_km,
     SUM(tt.trip_duration_mins) AS trip_duration_mins,
     SUM(tt.fare_amount) AS fares_amount,
     SUM(tt.tips_amount) AS tips_amount,
     SUM(tt.total_revenue_exc_tips) AS total_revenue_exc_tip

    FROM fct_taxi_trips tt
    GROUP BY date, payment_type
)

SELECT
    CAST(d.yearnum AS varchar) AS year,
    d.monthshortname AS month,
    d.monthnum AS month_num,
    d.dayname AS day,

    t.*,

    w.tempmax AS weather_daily_temp_high,
    w.tempmin AS weather_daily_temp_low
    
FROM agg_daily_trips t
LEFT JOIN dim_dates d
ON d.date = t.date
LEFT JOIN weather w
ON w.datetime = t.date
ORDER BY year, monthnum
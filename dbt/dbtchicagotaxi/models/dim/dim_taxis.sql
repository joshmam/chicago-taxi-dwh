-- purpose: create taxi dimension table from the taxi trips source data

WITH enh_taxi_trips AS(
    SELECT * FROM {{ ref('enh_taxi_trips') }}
)

SELECT DISTINCT
    taxi_id,
    company
FROM enh_taxi_trips
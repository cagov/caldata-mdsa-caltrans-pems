{{ config(materialized='table') }}

WITH DETECTOR_HEALTH AS (
    SELECT DISTINCT
        D.STATION_ID,
        D.DISTRICT,
        D.STATION_TYPE AS DETECTOR_TYPE,
        D.LANE,
        D.SAMPLE_DATE,
        D.STATUS
    FROM {{ ref('int_diagnostics__real_detector_status') }} AS D
    WHERE D.SAMPLE_DATE = '2024-07-18'
)

SELECT * FROM DETECTOR_HEALTH
LIMIT 1000

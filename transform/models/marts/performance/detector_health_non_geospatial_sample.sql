{{ config(materialized='table') }}

WITH DETECTOR_HEALTH AS (
    SELECT DISTINCT
        D.STATION_ID,
        D.DISTRICT,
        D.STATION_TYPE AS DETECTOR_TYPE,
        D.LANE,
        D.SAMPLE_DATE,
        D.STATUS,
        CASE
            WHEN D.STATUS = 'Good' THEN 'Good'
            ELSE 'Bad'
        END AS DETECTOR_STATUS
    FROM {{ ref('int_diagnostics__detector_status') }} AS D
    WHERE D.SAMPLE_DATE = DATEADD(DAY, -4, CAST(GETDATE() AS DATE))
)

SELECT * FROM DETECTOR_HEALTH
LIMIT 1000

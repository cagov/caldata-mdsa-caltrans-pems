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
),

GEO_LOC AS (
    SELECT DISTINCT
        V.STATION_ID,
        V.LATITUDE,
        V.LONGITUDE,
        V.DIRECTION,
        V.ABSOLUTE_POSTMILE
    FROM {{ ref('int_vds__detector_config') }} AS V
),

DETECTOR_HEALTH_WITH_LOC AS (
    SELECT
        DH.*,
        GL.LATITUDE,
        GL.LONGITUDE,
        GL.DIRECTION,
        GL.ABSOLUTE_POSTMILE
    FROM DETECTOR_HEALTH AS DH
    INNER JOIN GEO_LOC AS GL ON DH.STATION_ID = GL.STATION_ID
)

SELECT * FROM DETECTOR_HEALTH_WITH_LOC
LIMIT 1000
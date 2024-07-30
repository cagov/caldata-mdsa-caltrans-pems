{{ config(materialized='table') }}

WITH DETECTOR_HEALTH AS (
    SELECT DISTINCT
        D.STATION_ID,
        D.DISTRICT,
        D.TYPE AS DETECTOR_TYPE,
        D.LANE,
        D.SAMPLE_DATE,
        D.STATUS
    FROM {{ ref('int_diagnostics__real_detector_status') }} AS D
    WHERE D.SAMPLE_DATE = '2024-07-18'
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
),

DETECTOR_HEALTH_WITH_GEOGRAPHY AS (
    SELECT
        *,
        TO_GEOGRAPHY('POINT(' || LONGITUDE || ' ' || LATITUDE || ')') AS GEOGRAPHY
    FROM DETECTOR_HEALTH_WITH_LOC
    WHERE LATITUDE IS NOT null AND LONGITUDE IS NOT null
)

SELECT * FROM DETECTOR_HEALTH_WITH_GEOGRAPHY

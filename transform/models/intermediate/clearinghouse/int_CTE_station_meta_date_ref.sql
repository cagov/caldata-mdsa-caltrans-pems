{{ config(materialized='table') }}
with
source as (
    select * from {{ ref('cleaninghouse_station_meta') }} 
), 

station_meta_wth_date as (
select
    FILENAME,
    ID,
    FWY as FREEWAY,
    DIR as DIRECTION,
    DISTRICT,
    COUNTY,
    CITY,
    STATE_PM as STATE_POSTMILE,
    ABS_PM as ABSOLUTE_POSTMILE,
    LATITUDE,
    LONGITUDE,
    LENGTH,
    TYPE,
    LANES as NUMBER,
    NAME,
    USER_ID_1,
    USER_ID_2,
    USER_ID_3,
    USER_ID_4,
    DATE_FROM_PARTS(SUBSTR(FILENAME, 18, 4), SUBSTR(FILENAME, 23, 2), LEFT(RIGHT(FILENAME, 6), 2)) as META_DATE
    from source
)

select * from station_meta_wth_date
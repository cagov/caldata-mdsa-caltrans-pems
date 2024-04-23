{{ config(materialized='table') }}
with
COUNTY_DATA as (
    select
        ID,
        COUNTY,
        META_DATE
    from {{ ref('int_CTE_station_meta') }}
    where year(META_DATE) = 2023
--  where CAST(SUBSTRING(META_DATE, 1, 4) as int) = 2023
)

select
    COUNTY,
    cast(count(ID) as integer) as STATION_COUNTS
from COUNTY_DATA
group by COUNTY
order by STATION_COUNTS desc
limit 30
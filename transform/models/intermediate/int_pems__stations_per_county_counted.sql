with

county_data as (
    select
        county,
        COUNT(id) as station_counts
    from {{ ref("stg_pems__station_meta") }}
    where Year(meta_date) = 2023
    group by county
    order by station_counts asc
)

select *
from county_data
order by station_counts desc

-- select county, count(ID) as station_counts
-- from {{ ref("stg_pems__station_meta") }}
-- where  CAST(SUBSTRING(META_DATE, 1, 4) AS INT) = 2023
-- Group by COUNTY
-- Order by station_counts Desc
-- limit 10


-- select *
-- from {{ ref("stg_pems__station_meta") }}
-- limit 10

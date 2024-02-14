with

County_data as (
    select
        County,
        COUNT(Id) as Station_counts
    from {{ ref("stg_pems__station_meta") }}
    where Year(Meta_date) = 2023
    group by County
    order by Station_counts asc
)

select
    *
from County_data
order by Station_counts desc

-- select county, count(ID) as station_counts
-- from {{ ref("stg_pems__station_meta") }}
-- where  CAST(SUBSTRING(META_DATE, 1, 4) AS INT) = 2023
-- Group by COUNTY
-- Order by station_counts Desc
-- limit 10


-- select *
-- from {{ ref("stg_pems__station_meta") }}
-- limit 10

with

source as (
    select * from {{ ref("stg_pems__station_meta") }}
),

county_data as (
    select
        county,
        Count(id) as sta_counts
    from source
    where Year(meta_date) = 2023
    group by county

)

select *
from county_data
order by sta_counts desc


-- select *
-- from stg_pems__station_meta
-- limit 10

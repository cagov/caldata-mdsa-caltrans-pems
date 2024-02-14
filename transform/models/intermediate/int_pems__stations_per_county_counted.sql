with

source as (
    select * from {{ ref('stg_pems__station_meta') }}
),

stns_per_county as (
    select
        DISTINCT county,
        COUNT(id) as stn_count
    from source
    where YEAR(meta_date)::INT = 2023
    group by county
)

select * from stns_per_county order by stn_count desc

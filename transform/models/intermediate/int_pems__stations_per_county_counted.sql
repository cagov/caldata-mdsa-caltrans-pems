with

source as (
    select * from {{ ref("stg_pems__station_meta") }}
),

stations_per_county as (
    select
        county,
        COUNT(distinct id) as station_count
    from source
    where DATE_PART(year, meta_date)::INT = 2023
    group by county
)

select * from stations_per_county
order by station_count desc

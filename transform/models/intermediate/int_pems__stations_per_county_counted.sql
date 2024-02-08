with

staging_station_meta as (
    select * from {{ ref("stg_pems__station_meta") }}
),

stations_per_county_counted as (
    select
        county_name,
        COUNT(id) as station_count
    from staging_station_meta
    where meta_date between '2023-01-01' and '2023-12-31'
    -- normally this kind of filtering would be done in the BI layer
    group by county_name
)

select * from stations_per_county_counted
order by station_count desc

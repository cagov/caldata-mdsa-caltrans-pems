{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref("stg_clearinghouse__station_meta") }}
),

most_recent_station_meta as (
    select * exclude (filename)
    from station_meta
    where filename in (
        select max_by(filename, meta_date)
        from station_meta
        group by district
    )
)

select * from most_recent_station_meta

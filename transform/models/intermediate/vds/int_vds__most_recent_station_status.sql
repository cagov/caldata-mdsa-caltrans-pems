{{ config(materialized="table") }}

with station_status as (
    select * from {{ ref("stg_clearinghouse__station_status") }}
),

most_recent_station_status as (
    select * exclude (filename)
    from station_status
    where filename in (
        select max_by(filename, meta_date)
        from station_status
        group by district
    )
)

select * from most_recent_station_status

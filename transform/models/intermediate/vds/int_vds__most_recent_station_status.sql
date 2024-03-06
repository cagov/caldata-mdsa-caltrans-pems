{{ config(materialized="ephemeral") }}

with station_status as (
    select * from {{ ref("int_vds__station_status") }}
),

most_recent_station_status as (
    select * exclude (filename, _valid_from, _valid_to)
    from station_status
    where _valid_to is null
)

select * from most_recent_station_status

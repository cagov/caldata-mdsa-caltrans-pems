{{ config(materialized="ephemeral") }}

with station_meta as (
    select * from {{ ref("int_vds__station_meta") }}
),

most_recent_station_meta as (
    select * exclude (filename, _valid_from, _valid_to)
    from station_meta
    where _valid_to is null
)

select * from most_recent_station_meta

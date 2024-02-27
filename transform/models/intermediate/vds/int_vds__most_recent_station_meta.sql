{{ config(materialized="ephemeral") }}

with station_meta as (
    select * from {{ ref("int_vds__station_meta") }}
),

most_recent_station_meta as (
    select * exclude (filename)
    from station_meta
    where _valid_to is null
)

select * from most_recent_station_meta

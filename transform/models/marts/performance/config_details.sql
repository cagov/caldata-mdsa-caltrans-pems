{{ config(materialized='table') }}

with detector_conf as (
    select distinct
        station_id,
        latitude,
        longitude,
        direction,
        freeway,
        station_type,
        district
    from {{ ref('int_vds__station_config') }}
    where
        latitude is not null and longitude is not null
        and station_type in ('HV', 'ML')
)

select * from detector_conf

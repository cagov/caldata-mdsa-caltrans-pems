{{ config(materialized='table') }}

with detector_conf as (
    select distinct
        station_id,
        latitude,
        longitude,
        direction,
        station_type,
        angle,
        district
    from {{ ref('int_vds__station_config') }}
    where latitude is not null and longitude is not null
)

select * from detector_conf

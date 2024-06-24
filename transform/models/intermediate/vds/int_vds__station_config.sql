{{ config(materialized='table') }}

with config_log as (
    select * from {{ source('db96_dev', 'station_config_log') }}
),

config as (
    select * from {{ source('db96_dev', 'station_config') }}
),

config_log_with_validity as (
    select
        *,
        time_id as _valid_from,
        lead(time_id) over (partition by station_id order by time_id asc) as _valid_to
    from config_log
),

config_scd as (
    select
        config_log_with_validity.*,
        config.station_type,
        config.district_id as district,
        config.county_id as county,
        config.city_id as city,
        config.freeway_id as freeway,
        config.freeway_dir as direction,
        config.origin_set
    from config_log_with_validity
    left join config
        on config_log_with_validity.station_id = config.station_id
)

select * from config_scd

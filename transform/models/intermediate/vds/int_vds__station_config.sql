{{ config(materialized='table') }}

with config_log as (
    select * from {{ ref('stg_db96__station_config_log') }}
),

config as (
    select * from {{ ref('stg_db96__station_config') }}
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
        config.district,
        config.county,
        config.city,
        config.freeway,
        config.direction
    from config_log_with_validity
    left join config
        on config_log_with_validity.station_id = config.station_id
)

select * from config_scd

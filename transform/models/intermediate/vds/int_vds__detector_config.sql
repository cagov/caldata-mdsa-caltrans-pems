{{ config(materialized='table') }}

with config_log as (
    select * from {{ ref('stg_db96__detector_config_log') }}
),

config as (
    select * from {{ ref('stg_db96__detector_config') }}
),

station_config as (
    select * from {{ ref('int_vds__station_config') }}
),

config_log_with_validity as (
    select
        *,
        time_id as _valid_from,
        lead(time_id) over (partition by detector_id order by time_id asc) as _valid_to
    from config_log
),

config_scd as (
    select
        config_log_with_validity.detector_id,
        config_log_with_validity.station_id,
        config_log_with_validity.status,
        config_log_with_validity.lane,
        config.detector_type,
        station_config.station_type,
        station_config.district,
        station_config.county,
        station_config.city,
        station_config.freeway,
        station_config.direction,
        station_config.length,
        station_config.state_postmile,
        station_config.absolute_postmile,
        station_config.latitude,
        station_config.longitude,
        station_config.physical_lanes,
        config_log_with_validity._valid_from,
        config_log_with_validity._valid_to
    from config_log_with_validity
    left join config
        on config_log_with_validity.detector_id = config.detector_id
    left join station_config
        on
            config_log_with_validity.station_id = station_config.station_id
            and config_log_with_validity._valid_from >= station_config._valid_from
            and (config_log_with_validity._valid_from < station_config._valid_to or station_config._valid_to is null)
)

select * from config_scd

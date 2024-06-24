{{ config(materialized='table') }}

with config_log as (
    select * from {{ source('db96_dev', 'detector_config_log') }}
),

config as (
    select * from {{ source('db96_dev', 'detector_config') }}
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
        config_log_with_validity.*,
        config.detector_type as type,
        config.origin_set
    from config_log_with_validity
    left join config
        on config_log_with_validity.detector_id = config.detector_id
)

select * from config_scd

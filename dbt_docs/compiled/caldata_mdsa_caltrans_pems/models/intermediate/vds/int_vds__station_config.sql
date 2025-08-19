

with config_log as (
    select * from ANALYTICS_PRD.db96.stg_db96__station_config_log
),

config as (
    select * from ANALYTICS_PRD.db96.stg_db96__station_config
),

config_log_with_validity as (
    select
        * exclude (time_id),
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
    where config_log_with_validity.status = 1
)

select * from config_scd
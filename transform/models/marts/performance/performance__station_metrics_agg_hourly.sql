{{ config(
    materialized='incremental',
    snowflake_warehouse='TRANSFORMING_XL_DEV'
) }}

with hourly as (
    select
        station_id,
        sample_date,
        sample_hour,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        hourly_volume,
        hourly_occupancy,
        hourly_speed,
        hourly_vmt,
        hourly_vht,
        hourly_q_value,
        hourly_tti,
        county
    from {{ ref('int_performance__station_metrics_agg_hourly') }}
),

hourlyc as (
    {{ get_county_name('hourly') }}
),

hourlycc as (
    {{ get_city_name('hourlyc') }}
)

select * from hourlycc

{{ config(materialized='table') }}

with monthly as (
    select
        station_id,
        sample_month,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        monthly_volume,
        monthly_occupancy,
        monthly_speed,
        monthly_vmt,
        monthly_vht,
        monthly_q_value,
        monthly_tti,
        county
    from {{ ref('int_performance__station_metrics_agg_monthly') }}
),

monthlyc as (
    {{ get_county_name('monthly') }}
),

monthlycc as (
    {{ get_city_name('monthlyc') }}
)

select * from monthlycc

{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}

with daily as (
    select
        station_id,
        sample_date,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        daily_volume,
        daily_occupancy,
        daily_speed,
        daily_vmt,
        daily_vht,
        daily_q_value,
        daily_tti,
        county
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

dailyc as (
    {{ get_county_name('daily') }}
),

dailycc as (
    {{ get_city_name('dailyc') }}
)

select * from dailycc

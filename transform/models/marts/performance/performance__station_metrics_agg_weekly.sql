with weekly as (
    select
        station_id,
        sample_year,
        sample_week,
        sample_week_start_date,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        weekly_volume,
        weekly_occupancy,
        weekly_speed,
        weekly_vmt,
        weekly_vht,
        weekly_q_value,
        weekly_tti,
        county
    from {{ ref('int_performance__station_metrics_agg_weekly') }}
),

weeklyc as (
    {{ get_county_name('weekly') }}
),

weeklycc as (
    {{ get_city_name('weeklyc') }}
)

select * from weeklycc

{{ config(materialized='table') }}

with weekly_bottleneck_delay as (
    select
        station_id,
        sample_week,
        time_shift,
        cast(district as int) as district,
        station_type,
        freeway,
        direction,
        absolute_postmile,
        weekly_time_shift_duration,
        weekly_active_days,
        weekly_time_shift_extent,
        county
    from {{ ref('int_performance__bottleneck_delay_metrics_agg_weekly') }}
),

bottleneck_delay_with_county as (
    {{ get_county_name('weekly_bottleneck_delay') }}
),

geo as (
    select
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location
    from {{ ref('geo__current_stations') }}
),

bottleneck_delay_county_geo as (
    select
        bottleneck_delay_with_county.*,
        geo.latitude,
        geo.longitude,
        geo.location
    from
        bottleneck_delay_with_county
    inner join
        geo
        on bottleneck_delay_with_county.station_id = geo.station_id
)

select * from bottleneck_delay_county_geo

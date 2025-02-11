{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}


with daily_bottleneck as (
    select
        station_id,
        sample_date,
        time_shift,
        cast(district as int) as district,
        station_type,
        freeway,
        direction,
        absolute_postmile,
        daily_time_shift_duration,
        daily_time_shift_bottleneck_extent,
        county
    from {{ ref('int_performance__bottleneck_delay_metrics_agg_daily') }}
),

bottleneck_delay_with_county as (
    {{ get_county_name('daily_bottleneck') }}
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

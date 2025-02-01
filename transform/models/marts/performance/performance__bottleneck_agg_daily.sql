{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}


with daily_bottleneck_delay as (
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
        daily_time_shift_spatial_delay_35_mph,
        daily_time_shift_spatial_delay_40_mph,
        daily_time_shift_spatial_delay_45_mph,
        daily_time_shift_spatial_delay_50_mph,
        daily_time_shift_spatial_delay_55_mph,
        daily_time_shift_spatial_delay_60_mph,
        county
    from {{ ref('int_performance__bottleneck_delay_metrics_agg_daily') }}
),

-- unpivot delay
unpivot_delay as (
    select
        station_id,
        sample_date,
        time_shift,
        district,
        station_type,
        freeway,
        direction,
        absolute_postmile,
        daily_time_shift_duration,
        daily_time_shift_bottleneck_extent,
        county,
        regexp_substr(metric, '([0-9])+', 1, 1) as target_speed,
        value as delay
    from (
        select *
        from daily_bottleneck_delay
        unpivot (
            value for metric in (
                daily_time_shift_spatial_delay_35_mph,
                daily_time_shift_spatial_delay_40_mph,
                daily_time_shift_spatial_delay_45_mph,
                daily_time_shift_spatial_delay_50_mph,
                daily_time_shift_spatial_delay_55_mph,
                daily_time_shift_spatial_delay_60_mph
            )
        )
    )
),

bottleneck_delay_with_county as (
    {{ get_county_name('unpivot_delay') }}
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

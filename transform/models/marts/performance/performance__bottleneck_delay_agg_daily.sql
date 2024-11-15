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

bottleneck_delay_with_county as (
    {{ get_county_name('daily_bottleneck_delay') }}
),

geo as (
    select
        current_geo.station_id,
        current_geo.latitude,
        current_geo.longitude,
        concat(current_geo.longitude, ',', current_geo.latitude) as location,
        current_geo.absolute_postmile
    from
        {{ ref('geo__current_detectors') }} as current_geo
    where
        current_geo.absolute_postmile = (
            select max(sub.absolute_postmile)
            from {{ ref('geo__current_detectors') }} as sub
            where sub.station_id = current_geo.station_id
        )
    group by
        current_geo.station_id,
        current_geo.latitude,
        current_geo.longitude,
        current_geo.absolute_postmile
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

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
    from ANALYTICS_PRD.performance.int_performance__bottleneck_delay_metrics_agg_daily
),

bottleneck_delay_with_county as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            daily_bottleneck_delay.* exclude (county),
            c.county
        from daily_bottleneck_delay
        inner join county as c
        on daily_bottleneck_delay.county = c.county_id
    )

    select * from station_with_county

),

geo as (
    select
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location
    from ANALYTICS_PRD.geo.geo__current_stations
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
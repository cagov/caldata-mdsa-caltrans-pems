


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
    from ANALYTICS_PRD.performance.int_performance__bottleneck_delay_metrics_agg_daily
),

bottleneck_delay_with_county as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            daily_bottleneck.*,
            c.county_name,
            c.county_abb
        from daily_bottleneck
        inner join county as c
        on daily_bottleneck.county = c.county_id
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
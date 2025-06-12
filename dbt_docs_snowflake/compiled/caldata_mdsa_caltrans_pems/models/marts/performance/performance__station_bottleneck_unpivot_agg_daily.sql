

with daily_bottleneck_delay as (
    select * from ANALYTICS_PRD.performance.int_performance__bottleneck_delay_metrics_agg_daily
),

unpivot_delay as (
    select
        station_id,
        sample_date,
        time_shift,
        station_type,
        district,
        freeway,
        direction,
        absolute_postmile,
        county,
        target_speed,
        sum(coalesce(delay, 0)) as delay
    from (
        
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '35' as target_speed,
                daily_time_shift_spatial_delay_35_mph as delay
            from
                daily_bottleneck_delay
             union all 
        
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '40' as target_speed,
                daily_time_shift_spatial_delay_40_mph as delay
            from
                daily_bottleneck_delay
             union all 
        
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '45' as target_speed,
                daily_time_shift_spatial_delay_45_mph as delay
            from
                daily_bottleneck_delay
             union all 
        
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '50' as target_speed,
                daily_time_shift_spatial_delay_50_mph as delay
            from
                daily_bottleneck_delay
             union all 
        
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '55' as target_speed,
                daily_time_shift_spatial_delay_55_mph as delay
            from
                daily_bottleneck_delay
             union all 
        
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '60' as target_speed,
                daily_time_shift_spatial_delay_60_mph as delay
            from
                daily_bottleneck_delay
            
        
    ) as combined_metrics
    group by
        station_id,
        sample_date,
        time_shift,
        station_type,
        district,
        freeway,
        direction,
        absolute_postmile,
        county,
        target_speed
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
            unpivot_delay.*,
            c.county_name,
            c.county_abb
        from unpivot_delay
        inner join county as c
        on unpivot_delay.county = c.county_id
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
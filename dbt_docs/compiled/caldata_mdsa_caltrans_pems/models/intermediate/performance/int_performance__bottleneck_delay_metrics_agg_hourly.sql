

-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (
    select
        *,
        date_trunc('hour', sample_timestamp) as sample_timestamp_trunc
    from ANALYTICS_PRD.performance.int_performance__bottleneck_delay_metrics_agg_five_minutes
    where 
    1=1
    
),

-- aggregate five mins delay and calculate the average bottleneck extent in an hourly basis
hourly_spatial_bottleneck_delay_metrics as (
    select
        station_id,
        sample_date,
        sample_timestamp_trunc as sample_hour,
        any_value(district) as district,
        any_value(county) as county,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(absolute_postmile) as absolute_postmile,
        any_value(time_shift) as time_shift,
        sum(case when is_bottleneck = true then 1 else 0 end) * 5 as hourly_duration,
        avg(bottleneck_extent) as hourly_bottleneck_extent,
        -- spatial delay aggregation in hourly level
        
            sum(spatial_delay_35_mph)
                as hourly_spatial_delay_35_mph
            
                ,
            
        
            sum(spatial_delay_40_mph)
                as hourly_spatial_delay_40_mph
            
                ,
            
        
            sum(spatial_delay_45_mph)
                as hourly_spatial_delay_45_mph
            
                ,
            
        
            sum(spatial_delay_50_mph)
                as hourly_spatial_delay_50_mph
            
                ,
            
        
            sum(spatial_delay_55_mph)
                as hourly_spatial_delay_55_mph
            
                ,
            
        
            sum(spatial_delay_60_mph)
                as hourly_spatial_delay_60_mph
            
        
    from station_five_mins_data
    group by station_id, sample_date, sample_hour
)

select * from hourly_spatial_bottleneck_delay_metrics
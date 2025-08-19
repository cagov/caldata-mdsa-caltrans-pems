

with hourly_spatial_bottleneck_delay_metrics as (
    select *
    from ANALYTICS_PRD.performance.int_performance__bottleneck_delay_metrics_agg_hourly
    where 
    1=1
    
),

/*aggregate hourly delay and bottleneck extent in a daily level. Since one day has
3 time shifts, the aggregation would be in a time shift level*/

daily_time_shift_spatial_bottleneck_delay_metrics as (
    select
        station_id,
        sample_date,
        time_shift,
        any_value(district) as district,
        any_value(county) as county,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(absolute_postmile) as absolute_postmile,
        sum(hourly_duration) as daily_time_shift_duration,
        avg(hourly_bottleneck_extent) as daily_time_shift_bottleneck_extent,
        -- spatial delay aggregation in daily level, decomposed into time shift
        
            sum(hourly_spatial_delay_35_mph)
                as daily_time_shift_spatial_delay_35_mph
            
                ,
            
        
            sum(hourly_spatial_delay_40_mph)
                as daily_time_shift_spatial_delay_40_mph
            
                ,
            
        
            sum(hourly_spatial_delay_45_mph)
                as daily_time_shift_spatial_delay_45_mph
            
                ,
            
        
            sum(hourly_spatial_delay_50_mph)
                as daily_time_shift_spatial_delay_50_mph
            
                ,
            
        
            sum(hourly_spatial_delay_55_mph)
                as daily_time_shift_spatial_delay_55_mph
            
                ,
            
        
            sum(hourly_spatial_delay_60_mph)
                as daily_time_shift_spatial_delay_60_mph
            
        
    from hourly_spatial_bottleneck_delay_metrics
    where time_shift is not NULL
    group by station_id, sample_date, time_shift
)

select * from daily_time_shift_spatial_bottleneck_delay_metrics
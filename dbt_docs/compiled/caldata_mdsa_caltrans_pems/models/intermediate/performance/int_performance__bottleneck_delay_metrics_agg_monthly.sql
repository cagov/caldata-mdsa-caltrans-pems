

with station_daily_data as (
    select
        *,
        -- Extracting first day of each month
        -- reference: https://docs.snowflake.com/en/sql-reference/functions/year
        date_trunc(month, sample_date) as sample_month
    from ANALYTICS_PRD.performance.int_performance__bottleneck_delay_metrics_agg_daily
    where date_trunc(month, sample_date) != date_trunc(month, current_date)
),

monthly_spatial_bottleneck_delay_metrics as (
    select
        station_id,
        sample_month,
        time_shift,
        any_value(district) as district,
        any_value(county) as county,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(absolute_postmile) as absolute_postmile,
        avg(daily_time_shift_duration) as monthly_time_shift_duration,
        sum(case when daily_time_shift_duration > 0 then 1 else 0 end) as monthly_active_days,
        avg(daily_time_shift_bottleneck_extent) as monthly_time_shift_extent,
        -- spatial delay aggregation in monthly level, decomposed into time shift
        
            sum(daily_time_shift_spatial_delay_35_mph)
                as monthly_time_shift_spatial_delay_35_mph
            
                ,
            
        
            sum(daily_time_shift_spatial_delay_40_mph)
                as monthly_time_shift_spatial_delay_40_mph
            
                ,
            
        
            sum(daily_time_shift_spatial_delay_45_mph)
                as monthly_time_shift_spatial_delay_45_mph
            
                ,
            
        
            sum(daily_time_shift_spatial_delay_50_mph)
                as monthly_time_shift_spatial_delay_50_mph
            
                ,
            
        
            sum(daily_time_shift_spatial_delay_55_mph)
                as monthly_time_shift_spatial_delay_55_mph
            
                ,
            
        
            sum(daily_time_shift_spatial_delay_60_mph)
                as monthly_time_shift_spatial_delay_60_mph
            
        
    from station_daily_data
    group by station_id, sample_month, time_shift
)

select * from monthly_spatial_bottleneck_delay_metrics
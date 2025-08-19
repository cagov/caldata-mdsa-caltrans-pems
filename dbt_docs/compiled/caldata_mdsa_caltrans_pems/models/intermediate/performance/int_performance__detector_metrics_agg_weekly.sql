

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting the first day of the week date, and week number
        -- reference: https://docs.snowflake.com/en/sql-reference/functions-date-time#label-calendar-weeks-weekdays
        weekofyear(sample_date) as sample_week,
        date_trunc('week', sample_date) as sample_week_start_date
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_daily
    -- we do not want to calculate incomplete week aggregation
    where date_trunc(week, sample_date) != date_trunc(week, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly
weekly_spatial_temporal_metrics as (
    select
        detector_id,
        sample_week,
        sample_week_start_date,
        any_value(station_id) as station_id,
        any_value(lane) as lane,
        any_value(city) as city,
        any_value(county) as county,
        any_value(district) as district,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(daily_volume) as weekly_volume,
        avg(daily_occupancy) as weekly_occupancy,
        sum(daily_vmt) as weekly_vmt,
        sum(daily_vht) as weekly_vht,
        weekly_vmt / nullifzero(weekly_vht) as weekly_q_value,
        -- travel time
        60 / nullifzero(weekly_q_value) as weekly_tti,
        
            sum(delay_35_mph)
                as delay_35_mph
            
                ,
            

        
            sum(delay_40_mph)
                as delay_40_mph
            
                ,
            

        
            sum(delay_45_mph)
                as delay_45_mph
            
                ,
            

        
            sum(delay_50_mph)
                as delay_50_mph
            
                ,
            

        
            sum(delay_55_mph)
                as delay_55_mph
            
                ,
            

        
            sum(delay_60_mph)
                as delay_60_mph
            

        ,
        
            sum(lost_productivity_35_mph)
                as lost_productivity_35_mph
            
                ,
            

        
            sum(lost_productivity_40_mph)
                as lost_productivity_40_mph
            
                ,
            

        
            sum(lost_productivity_45_mph)
                as lost_productivity_45_mph
            
                ,
            

        
            sum(lost_productivity_50_mph)
                as lost_productivity_50_mph
            
                ,
            

        
            sum(lost_productivity_55_mph)
                as lost_productivity_55_mph
            
                ,
            

        
            sum(lost_productivity_60_mph)
                as lost_productivity_60_mph
            

        
    from station_daily_data
    group by
        detector_id, sample_week, sample_week_start_date
)

select * from weekly_spatial_temporal_metrics
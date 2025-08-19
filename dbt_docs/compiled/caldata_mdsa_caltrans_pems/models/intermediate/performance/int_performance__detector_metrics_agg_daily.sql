

-- read the station hourly data
with station_hourly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_hourly
    where 
    1=1
    
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_spatial_temporal_agg as (
    select
        detector_id,
        sample_date,
        any_value(station_id) as station_id,
        any_value(station_type) as station_type,
        any_value(lane) as lane,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(hourly_volume) as daily_volume,
        avg(hourly_occupancy) as daily_occupancy,
        sum(hourly_volume * hourly_speed) / nullifzero(sum(hourly_volume)) as daily_speed,
        sum(hourly_vmt) as daily_vmt,
        sum(hourly_vht) as daily_vht,
        daily_vmt / nullifzero(daily_vht) as daily_q_value,
        -- travel time
        60 / nullifzero(daily_q_value) as daily_tti,
        
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
            

        
    from station_hourly_data
    group by detector_id, sample_date
)

select * from daily_spatial_temporal_agg
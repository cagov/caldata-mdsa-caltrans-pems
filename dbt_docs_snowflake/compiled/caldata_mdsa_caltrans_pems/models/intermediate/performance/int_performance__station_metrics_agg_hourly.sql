
-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (
    select
        *,
        date_trunc('hour', sample_timestamp) as sample_timestamp_trunc
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_five_minutes
    where 
    1=1
    
),

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_station_temporal_metrics as (
    select
        station_id,
        sample_date,
        sample_timestamp_trunc as sample_hour,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(length) as length,
        sum(volume_sum) as hourly_volume,
        avg(occupancy_avg) as hourly_occupancy,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as hourly_speed,
        sum(vmt) as hourly_vmt,
        sum(vht) as hourly_vht,
        hourly_vmt / nullifzero(hourly_vht) as hourly_q_value,
        -- travel time
        60 / nullifzero(hourly_q_value) as hourly_tti,
        
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / 35)), 0
            )
                as delay_35_mph
            
                ,
            
        
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / 40)), 0
            )
                as delay_40_mph
            
                ,
            
        
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / 45)), 0
            )
                as delay_45_mph
            
                ,
            
        
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / 50)), 0
            )
                as delay_50_mph
            
                ,
            
        
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / 55)), 0
            )
                as delay_55_mph
            
                ,
            
        
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / 60)), 0
            )
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
            
        
    from station_five_mins_data
    group by station_id, sample_date, sample_hour
)

select * from hourly_station_temporal_metrics
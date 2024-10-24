

with detector_agg_five_minutes as (
    select *
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_five_minutes
    where 
    1=1
    
),

station_aggregated as (
    select
        station_id,
        sample_date,
        sample_timestamp,
        any_value(absolute_postmile) as absolute_postmile,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(length) as length,
        any_value(station_valid_from) as station_valid_from,
        any_value(station_valid_to) as station_valid_to,
        round(sum(vmt), 1) as vmt,
        round(sum(vht), 2) as vht,
        sum(sample_ct) as sample_ct,
        round(sum(volume_sum), 0) as volume_sum,
        round(avg(occupancy_avg), 4) as occupancy_avg,
        round(sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)), 1) as speed_five_mins,
        
            round(sum(lost_productivity_35_mph), 2) as lost_productivity_35_mph
            
                ,
            
        
            round(sum(lost_productivity_40_mph), 2) as lost_productivity_40_mph
            
                ,
            
        
            round(sum(lost_productivity_45_mph), 2) as lost_productivity_45_mph
            
                ,
            
        
            round(sum(lost_productivity_50_mph), 2) as lost_productivity_50_mph
            
                ,
            
        
            round(sum(lost_productivity_55_mph), 2) as lost_productivity_55_mph
            
                ,
            
        
            round(sum(lost_productivity_60_mph), 2) as lost_productivity_60_mph
            
        
    from detector_agg_five_minutes
    group by station_id, sample_date, sample_timestamp
),

station_aggregated_with_delay as (
    select
        *,
        
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / 35)), 0), 2)
                as delay_35_mph
            
                ,
            
        
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / 40)), 0), 2)
                as delay_40_mph
            
                ,
            
        
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / 45)), 0), 2)
                as delay_45_mph
            
                ,
            
        
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / 50)), 0), 2)
                as delay_50_mph
            
                ,
            
        
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / 55)), 0), 2)
                as delay_55_mph
            
                ,
            
        
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / 60)), 0), 2)
                as delay_60_mph
            
        
    from station_aggregated
)

select * from station_aggregated_with_delay
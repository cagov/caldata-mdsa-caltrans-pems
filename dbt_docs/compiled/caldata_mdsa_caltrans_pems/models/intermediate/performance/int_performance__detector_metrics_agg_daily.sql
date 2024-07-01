

-- read the station hourly data
with station_hourly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_hourly
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_spatial_temporal_agg as (
    select
        id,
        lane,
        sample_date,
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
    group by id, sample_date, lane
),

daily_spatial_temporal_metrics as (
    select
        dst.*,
        sm.type,
        sm.district,
        sm.city,
        sm.county,
        sm.freeway,
        sm.direction
    from daily_spatial_temporal_agg as dst
    inner join ANALYTICS_PRD.clearinghouse.int_clearinghouse__station_meta as sm
        on
            dst.id = sm.id
            and dst.sample_date >= sm._valid_from
            and
            (
                dst.sample_date < sm._valid_to
                or sm._valid_to is null
            )
)

select * from daily_spatial_temporal_metrics
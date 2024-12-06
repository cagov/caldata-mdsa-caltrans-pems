

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        year(sample_date) as sample_year
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_daily
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        sample_year,
        station_type,
        freeway,
        direction,
        sum(daily_volume) as yearly_volume_sum,
        avg(daily_occupancy) as yearly_occupancy_avg,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as yearly_speed_avg,
        sum(daily_vmt) as yearly_vmt,
        sum(daily_vht) as yearly_vht,
        yearly_vmt / nullifzero(yearly_vht) as yearly_q_value,
        -- travel time
        60 / nullifzero(yearly_q_value) as yearly_tti,
        
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
        sample_year, freeway, station_type, direction
)

select * from spatial_metrics
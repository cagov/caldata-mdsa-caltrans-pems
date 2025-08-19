

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting first day of each month
        -- reference: https://docs.snowflake.com/en/sql-reference/functions/year
        date_trunc(month, sample_date) as sample_month
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_daily
    where date_trunc(month, sample_date) != date_trunc(month, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly.
monthly_station_level_spatial_temporal_metrics as (
    select
        station_id,
        sample_month,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(length) as length,
        sum(daily_volume) as monthly_volume,
        avg(daily_occupancy) as monthly_occupancy,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as monthly_speed,
        sum(daily_vmt) as monthly_vmt,
        sum(daily_vht) as monthly_vht,
        monthly_vmt / nullifzero(monthly_vht) as monthly_q_value,
        -- travel time
        60 / nullifzero(monthly_q_value) as monthly_tti,
        
            greatest(
                monthly_volume
                * ((any_value(length) / nullifzero(monthly_speed)) - (any_value(length) / 35)),
                0
            )
                as delay_35_mph
            
                ,
            

        
            greatest(
                monthly_volume
                * ((any_value(length) / nullifzero(monthly_speed)) - (any_value(length) / 40)),
                0
            )
                as delay_40_mph
            
                ,
            

        
            greatest(
                monthly_volume
                * ((any_value(length) / nullifzero(monthly_speed)) - (any_value(length) / 45)),
                0
            )
                as delay_45_mph
            
                ,
            

        
            greatest(
                monthly_volume
                * ((any_value(length) / nullifzero(monthly_speed)) - (any_value(length) / 50)),
                0
            )
                as delay_50_mph
            
                ,
            

        
            greatest(
                monthly_volume
                * ((any_value(length) / nullifzero(monthly_speed)) - (any_value(length) / 55)),
                0
            )
                as delay_55_mph
            
                ,
            

        
            greatest(
                monthly_volume
                * ((any_value(length) / nullifzero(monthly_speed)) - (any_value(length) / 60)),
                0
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
            

        
    from station_daily_data
    group by station_id, sample_month
)

select * from monthly_station_level_spatial_temporal_metrics
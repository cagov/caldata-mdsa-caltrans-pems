

-- read the volume, occupancy and speed weekly level data
with station_weekly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_weekly
),

unpivot_combined as (
    select
        district,
        sample_week,
        sample_week_start_date,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                district,
                sample_week,
                sample_week_start_date,
                '35' as target_speed,
                nullif(delay_35_mph, 0) as delay,
                nullif(lost_productivity_35_mph, 0) as lost_productivity
            from
                station_weekly_data
             union all 
        
            select
                district,
                sample_week,
                sample_week_start_date,
                '40' as target_speed,
                nullif(delay_40_mph, 0) as delay,
                nullif(lost_productivity_40_mph, 0) as lost_productivity
            from
                station_weekly_data
             union all 
        
            select
                district,
                sample_week,
                sample_week_start_date,
                '45' as target_speed,
                nullif(delay_45_mph, 0) as delay,
                nullif(lost_productivity_45_mph, 0) as lost_productivity
            from
                station_weekly_data
             union all 
        
            select
                district,
                sample_week,
                sample_week_start_date,
                '50' as target_speed,
                nullif(delay_50_mph, 0) as delay,
                nullif(lost_productivity_50_mph, 0) as lost_productivity
            from
                station_weekly_data
             union all 
        
            select
                district,
                sample_week,
                sample_week_start_date,
                '55' as target_speed,
                nullif(delay_55_mph, 0) as delay,
                nullif(lost_productivity_55_mph, 0) as lost_productivity
            from
                station_weekly_data
             union all 
        
            select
                district,
                sample_week,
                sample_week_start_date,
                '60' as target_speed,
                nullif(delay_60_mph, 0) as delay,
                nullif(lost_productivity_60_mph, 0) as lost_productivity
            from
                station_weekly_data
            
        
    ) as combined_metrics
    group by
        district, sample_week, sample_week_start_date, target_speed
)

select * from unpivot_combined
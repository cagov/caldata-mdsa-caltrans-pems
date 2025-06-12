

-- read the volume, occupancy and speed yearly level data
with station_yearly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_yearly
),

-- aggregate delay and productivity by sample year
spatial_metrics as (
    select
        district,
        sample_year,
        
            sum(delay_35_mph) as delay_35_mph,
            sum(lost_productivity_35_mph) as lost_productivity_35_mph
            
                ,
            
        
            sum(delay_40_mph) as delay_40_mph,
            sum(lost_productivity_40_mph) as lost_productivity_40_mph
            
                ,
            
        
            sum(delay_45_mph) as delay_45_mph,
            sum(lost_productivity_45_mph) as lost_productivity_45_mph
            
                ,
            
        
            sum(delay_50_mph) as delay_50_mph,
            sum(lost_productivity_50_mph) as lost_productivity_50_mph
            
                ,
            
        
            sum(delay_55_mph) as delay_55_mph,
            sum(lost_productivity_55_mph) as lost_productivity_55_mph
            
                ,
            
        
            sum(delay_60_mph) as delay_60_mph,
            sum(lost_productivity_60_mph) as lost_productivity_60_mph
            
        
    from station_yearly_data
    group by
        district, sample_year
),

unpivot_combined as (
    select
        district,
        sample_year,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                district,
                sample_year,
                '35' as target_speed,
                nullif(delay_35_mph, 0) as delay,
                nullif(lost_productivity_35_mph, 0) as lost_productivity
            from
                spatial_metrics
             union all 
        
            select
                district,
                sample_year,
                '40' as target_speed,
                nullif(delay_40_mph, 0) as delay,
                nullif(lost_productivity_40_mph, 0) as lost_productivity
            from
                spatial_metrics
             union all 
        
            select
                district,
                sample_year,
                '45' as target_speed,
                nullif(delay_45_mph, 0) as delay,
                nullif(lost_productivity_45_mph, 0) as lost_productivity
            from
                spatial_metrics
             union all 
        
            select
                district,
                sample_year,
                '50' as target_speed,
                nullif(delay_50_mph, 0) as delay,
                nullif(lost_productivity_50_mph, 0) as lost_productivity
            from
                spatial_metrics
             union all 
        
            select
                district,
                sample_year,
                '55' as target_speed,
                nullif(delay_55_mph, 0) as delay,
                nullif(lost_productivity_55_mph, 0) as lost_productivity
            from
                spatial_metrics
             union all 
        
            select
                district,
                sample_year,
                '60' as target_speed,
                nullif(delay_60_mph, 0) as delay,
                nullif(lost_productivity_60_mph, 0) as lost_productivity
            from
                spatial_metrics
            
        
    ) as combined_metrics
    group by
        district, sample_year, target_speed
)

select * from unpivot_combined
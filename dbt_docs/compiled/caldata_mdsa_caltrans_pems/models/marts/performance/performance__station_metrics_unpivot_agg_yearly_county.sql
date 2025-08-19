

-- read the volume, occupancy and speed yearly level data
with station_yearly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_yearly
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        county,
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
        county, sample_year
),

unpivot_metrics as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            spatial_metrics.*,
            c.county_name,
            c.county_abb
        from spatial_metrics
        inner join county as c
        on spatial_metrics.county = c.county_id
    )

    select * from station_with_county

),

unpivot_combined as (
    select
        county,
        sample_year,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                county,
                sample_year,
                '35' as target_speed,
                nullif(delay_35_mph, 0) as delay,
                nullif(lost_productivity_35_mph, 0) as lost_productivity
            from
                unpivot_metrics
             union all 
        
            select
                county,
                sample_year,
                '40' as target_speed,
                nullif(delay_40_mph, 0) as delay,
                nullif(lost_productivity_40_mph, 0) as lost_productivity
            from
                unpivot_metrics
             union all 
        
            select
                county,
                sample_year,
                '45' as target_speed,
                nullif(delay_45_mph, 0) as delay,
                nullif(lost_productivity_45_mph, 0) as lost_productivity
            from
                unpivot_metrics
             union all 
        
            select
                county,
                sample_year,
                '50' as target_speed,
                nullif(delay_50_mph, 0) as delay,
                nullif(lost_productivity_50_mph, 0) as lost_productivity
            from
                unpivot_metrics
             union all 
        
            select
                county,
                sample_year,
                '55' as target_speed,
                nullif(delay_55_mph, 0) as delay,
                nullif(lost_productivity_55_mph, 0) as lost_productivity
            from
                unpivot_metrics
             union all 
        
            select
                county,
                sample_year,
                '60' as target_speed,
                nullif(delay_60_mph, 0) as delay,
                nullif(lost_productivity_60_mph, 0) as lost_productivity
            from
                unpivot_metrics
            
        
    ) as combined_metrics
    group by
        county, sample_year, target_speed
),

unpivot_combinedc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            unpivot_combined.*,
            c.county_name,
            c.county_abb
        from unpivot_combined
        inner join county as c
        on unpivot_combined.county = c.county_id
    )

    select * from station_with_county

)

select * from unpivot_combinedc
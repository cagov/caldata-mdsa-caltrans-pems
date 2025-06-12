-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_daily
),

dailyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            station_daily_data.*,
            c.county_name,
            c.county_abb
        from station_daily_data
        inner join county as c
        on station_daily_data.county = c.county_id
    )

    select * from station_with_county

),

unpivot_combined as (
    select
        county,
        county_abb,
        county_name,
        sample_date,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                county,
                county_abb,
                county_name,
                sample_date,
                '35' as target_speed,
                nullif(delay_35_mph, 0) as delay,
                nullif(lost_productivity_35_mph, 0) as lost_productivity
            from
                dailyc
             union all 
        
            select
                county,
                county_abb,
                county_name,
                sample_date,
                '40' as target_speed,
                nullif(delay_40_mph, 0) as delay,
                nullif(lost_productivity_40_mph, 0) as lost_productivity
            from
                dailyc
             union all 
        
            select
                county,
                county_abb,
                county_name,
                sample_date,
                '45' as target_speed,
                nullif(delay_45_mph, 0) as delay,
                nullif(lost_productivity_45_mph, 0) as lost_productivity
            from
                dailyc
             union all 
        
            select
                county,
                county_abb,
                county_name,
                sample_date,
                '50' as target_speed,
                nullif(delay_50_mph, 0) as delay,
                nullif(lost_productivity_50_mph, 0) as lost_productivity
            from
                dailyc
             union all 
        
            select
                county,
                county_abb,
                county_name,
                sample_date,
                '55' as target_speed,
                nullif(delay_55_mph, 0) as delay,
                nullif(lost_productivity_55_mph, 0) as lost_productivity
            from
                dailyc
             union all 
        
            select
                county,
                county_abb,
                county_name,
                sample_date,
                '60' as target_speed,
                nullif(delay_60_mph, 0) as delay,
                nullif(lost_productivity_60_mph, 0) as lost_productivity
            from
                dailyc
            
        
    ) as combined_metrics
    group by
        county, county_abb, county_name, sample_date, target_speed
)

select * from unpivot_combined
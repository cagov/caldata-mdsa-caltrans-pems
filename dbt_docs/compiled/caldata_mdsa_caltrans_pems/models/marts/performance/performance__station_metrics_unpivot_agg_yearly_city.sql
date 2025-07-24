

-- read the volume, occupancy and speed yearly level data
with station_yearly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_yearly
),

yearlyc as (
    
    with city as (
        select
            city_id,
            city_name,
            native_id
        from ANALYTICS_PRD.analytics.cities
    ),
    station_with_city_id as (
        select
            st.*,
            c.city_name,
            c.native_id as city_abb
        from station_yearly_data as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

),

unpivot_combined as (
    select
        city,
        city_abb,
        city_name,
        sample_year,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                city,
                city_abb,
                city_name,
                sample_year,
                '35' as target_speed,
                nullif(delay_35_mph, 0) as delay,
                nullif(lost_productivity_35_mph, 0) as lost_productivity
            from
                yearlyc
             union all 
        
            select
                city,
                city_abb,
                city_name,
                sample_year,
                '40' as target_speed,
                nullif(delay_40_mph, 0) as delay,
                nullif(lost_productivity_40_mph, 0) as lost_productivity
            from
                yearlyc
             union all 
        
            select
                city,
                city_abb,
                city_name,
                sample_year,
                '45' as target_speed,
                nullif(delay_45_mph, 0) as delay,
                nullif(lost_productivity_45_mph, 0) as lost_productivity
            from
                yearlyc
             union all 
        
            select
                city,
                city_abb,
                city_name,
                sample_year,
                '50' as target_speed,
                nullif(delay_50_mph, 0) as delay,
                nullif(lost_productivity_50_mph, 0) as lost_productivity
            from
                yearlyc
             union all 
        
            select
                city,
                city_abb,
                city_name,
                sample_year,
                '55' as target_speed,
                nullif(delay_55_mph, 0) as delay,
                nullif(lost_productivity_55_mph, 0) as lost_productivity
            from
                yearlyc
             union all 
        
            select
                city,
                city_abb,
                city_name,
                sample_year,
                '60' as target_speed,
                nullif(delay_60_mph, 0) as delay,
                nullif(lost_productivity_60_mph, 0) as lost_productivity
            from
                yearlyc
            
        
    ) as combined_metrics
    where
        city is not null
    group by
        city, city_abb, city_name, sample_year, target_speed
)

select * from unpivot_combined
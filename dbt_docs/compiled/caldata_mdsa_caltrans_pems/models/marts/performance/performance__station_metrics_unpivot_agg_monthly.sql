

with monthly as (
    select * from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_monthly
),

monthlyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            monthly.*,
            c.county_name,
            c.county_abb
        from monthly
        inner join county as c
        on monthly.county = c.county_id
    )

    select * from station_with_county

),

monthlycc as (
    
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
        from monthlyc as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

),

unpivot_combined as (
    select
        station_id,
        sample_month,
        length,
        station_type,
        district,
        city,
        city_abb,
        city_name,
        freeway,
        direction,
        county,
        county_abb,
        county_name,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '35' as target_speed,
                delay_35_mph as delay,
                lost_productivity_35_mph as lost_productivity
            from
                monthlycc
             union all 
        
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '40' as target_speed,
                delay_40_mph as delay,
                lost_productivity_40_mph as lost_productivity
            from
                monthlycc
             union all 
        
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '45' as target_speed,
                delay_45_mph as delay,
                lost_productivity_45_mph as lost_productivity
            from
                monthlycc
             union all 
        
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '50' as target_speed,
                delay_50_mph as delay,
                lost_productivity_50_mph as lost_productivity
            from
                monthlycc
             union all 
        
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '55' as target_speed,
                delay_55_mph as delay,
                lost_productivity_55_mph as lost_productivity
            from
                monthlycc
             union all 
        
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '60' as target_speed,
                delay_60_mph as delay,
                lost_productivity_60_mph as lost_productivity
            from
                monthlycc
            
        
    ) as combined_metrics
    group by
        sample_month,
        station_id,
        length,
        station_type,
        district,
        city,
        city_abb,
        city_name,
        freeway,
        direction,
        county,
        county_abb,
        county_name,
        target_speed
)

select * from unpivot_combined


with daily as (
    select * from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_daily
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
            daily.*,
            c.county_name,
            c.county_abb
        from daily
        inner join county as c
        on daily.county = c.county_id
    )

    select * from station_with_county

),

dailycc as (
    
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
        from dailyc as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

),

unpivot_combined as (
    select
        station_id,
        sample_date,
        length,
        station_type,
        district,
        city,
        city_abb,
        city_name,
        freeway,
        direction,
        county,
        county_name,
        county_abb,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        
            select
                station_id,
                sample_date,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_name,
                county_abb,
                '35' as target_speed,
                delay_35_mph as delay,
                lost_productivity_35_mph as lost_productivity
            from
                dailycc
             union all 
        
            select
                station_id,
                sample_date,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_name,
                county_abb,
                '40' as target_speed,
                delay_40_mph as delay,
                lost_productivity_40_mph as lost_productivity
            from
                dailycc
             union all 
        
            select
                station_id,
                sample_date,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_name,
                county_abb,
                '45' as target_speed,
                delay_45_mph as delay,
                lost_productivity_45_mph as lost_productivity
            from
                dailycc
             union all 
        
            select
                station_id,
                sample_date,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_name,
                county_abb,
                '50' as target_speed,
                delay_50_mph as delay,
                lost_productivity_50_mph as lost_productivity
            from
                dailycc
             union all 
        
            select
                station_id,
                sample_date,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_name,
                county_abb,
                '55' as target_speed,
                delay_55_mph as delay,
                lost_productivity_55_mph as lost_productivity
            from
                dailycc
             union all 
        
            select
                station_id,
                sample_date,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_name,
                county_abb,
                '60' as target_speed,
                delay_60_mph as delay,
                lost_productivity_60_mph as lost_productivity
            from
                dailycc
            
        
    ) as combined_metrics
    group by
        sample_date,
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
        county_name,
        county_abb,
        target_speed
)

select * from unpivot_combined
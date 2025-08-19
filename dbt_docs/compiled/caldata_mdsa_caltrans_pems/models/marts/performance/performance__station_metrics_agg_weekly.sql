

with weekly as (
    select
        station_id,
        sample_year,
        sample_week,
        sample_week_start_date,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        weekly_volume,
        weekly_occupancy,
        weekly_speed,
        weekly_vmt,
        weekly_vht,
        weekly_q_value,
        weekly_tti,
        county
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_weekly
),

weeklyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            weekly.*,
            c.county_name,
            c.county_abb
        from weekly
        inner join county as c
        on weekly.county = c.county_id
    )

    select * from station_with_county

),

weeklycc as (
    
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
        from weeklyc as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

)

select * from weeklycc
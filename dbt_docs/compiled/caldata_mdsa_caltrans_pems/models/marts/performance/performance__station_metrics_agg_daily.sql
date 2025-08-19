

with daily as (
    select
        station_id,
        sample_date,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        daily_volume,
        daily_occupancy,
        daily_speed,
        daily_vmt,
        daily_vht,
        daily_q_value,
        daily_tti,
        county
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

)

select * from dailycc
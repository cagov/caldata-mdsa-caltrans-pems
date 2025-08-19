

with monthly as (
    select
        station_id,
        sample_month,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        monthly_volume,
        monthly_occupancy,
        monthly_speed,
        monthly_vmt,
        monthly_vht,
        monthly_q_value,
        monthly_tti,
        county
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_monthly
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

)

select * from monthlycc
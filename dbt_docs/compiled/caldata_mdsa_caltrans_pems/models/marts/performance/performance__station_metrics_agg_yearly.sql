

with yearly as (
    select
        station_id,
        sample_year,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        yearly_volume,
        yearly_occupancy,
        yearly_speed,
        yearly_vmt,
        yearly_vht,
        yearly_q_value,
        yearly_tti,
        county
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_yearly
),

yearlyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            yearly.*,
            c.county_name,
            c.county_abb
        from yearly
        inner join county as c
        on yearly.county = c.county_id
    )

    select * from station_with_county

),

yearlycc as (
    
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
        from yearlyc as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

)

select * from yearlycc
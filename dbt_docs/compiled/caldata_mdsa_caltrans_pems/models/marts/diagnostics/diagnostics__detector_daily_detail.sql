

with

detector_status as (
    select * from ANALYTICS_PRD.diagnostics.int_diagnostics__detector_status
    where sample_date is not null and lane is not null
),

detector_statusc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            detector_status.*,
            c.county_name,
            c.county_abb
        from detector_status
        inner join county as c
        on detector_status.county = c.county_id
    )

    select * from station_with_county

),

detector_statuscc as (
    
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
        from detector_statusc as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

)

select * from detector_statuscc



with imputation_five_mins as (
    select *
    from ANALYTICS_PRD.imputation.int_imputation__detector_imputed_agg_five_minutes
    where
        station_type in ('ML', 'HV')
        and sample_date >= dateadd(day, -4, current_date)
),

imputation_five_minsc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            imputation_five_mins.*,
            c.county_name,
            c.county_abb
        from imputation_five_mins
        inner join county as c
        on imputation_five_mins.county = c.county_id
    )

    select * from station_with_county

),

imputation_five_minscc as (
    
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
        from imputation_five_minsc as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

)

select * from imputation_five_minscc
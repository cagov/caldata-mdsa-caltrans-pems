


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

)

select * from imputation_five_minsc
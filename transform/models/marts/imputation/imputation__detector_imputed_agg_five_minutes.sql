with imputation_five_mins as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where
        station_type in ('ML', 'HV')
        and sample_date >= dateadd(day, -2, current_date)
)

select * from imputation_five_mins

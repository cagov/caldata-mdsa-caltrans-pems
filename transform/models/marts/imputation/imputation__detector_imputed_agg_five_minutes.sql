with imputation_five_mins as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where
        station_type in ('ML', 'HV')
),

imputation_five_minsc as (
    {{ get_county_name('imputation_five_mins') }}
)

select * from imputation_five_minsc

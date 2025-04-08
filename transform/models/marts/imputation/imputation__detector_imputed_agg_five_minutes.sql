{{ config(
    materialized="table",
    unload_partitioning="('day=' || to_varchar(date_part(day, sample_date)) || '/district=' || district)",
) }}


with imputation_five_mins as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where
        station_type in ('ML', 'HV')
        and sample_date >= dateadd(day, -4, current_date)
),

imputation_five_minsc as (
    {{ get_county_name('imputation_five_mins') }}
),

imputation_five_minscc as (
    {{ get_city_name('imputation_five_minsc') }}
)

select * from imputation_five_minscc

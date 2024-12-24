{{ config(
    materialized="incremental",
    unique_key=["sample_date", "sample_timestamp", "detector_id"],
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)) || '/day=' || to_varchar(date_part(day, sample_date)))",
    unload_filter="sample_date >= dateadd(day, -2, current_date())"
) }}


with imputation_five_mins as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where
        station_type in ('ML', 'HV')
        and {{ make_model_incremental('sample_date') }}
),

imputation_five_minsc as (
    {{ get_county_name('imputation_five_mins') }}
)

select * from imputation_five_minsc

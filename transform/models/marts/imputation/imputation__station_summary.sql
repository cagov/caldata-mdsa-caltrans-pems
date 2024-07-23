{{ config(
    materialized="incremental",
    unique_key=['station_id', 'sample_date'],
    snowflake_warehouse=get_snowflake_warehouse(size="XL")
) }}

-- read observed and imputed five minutes data
with obs_imputed_five_minutes_agg as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

imputation_count as (
    select
        station_id,
        district,
        sample_date,
        count(station_id) as sample_ct,
        count(case when occupancy_imputation_method = 'local' then station_id end) as number_local_imputation,
        count(case when occupancy_imputation_method = 'regional' then station_id end) as number_regional_imputation,
        count(case when occupancy_imputation_method = 'global' then station_id end) as number_global_imputation,
        count(case when occupancy_imputation_method = 'observed' then station_id end) as number_observed_sample,
        count(case when occupancy_imputation_method is NULL then station_id end) as number_unobserved_unimputed
    from obs_imputed_five_minutes_agg
    group by station_id, district, sample_date
)

select * from imputation_count

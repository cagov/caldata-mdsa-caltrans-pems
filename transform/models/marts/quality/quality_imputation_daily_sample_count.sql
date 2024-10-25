{{ config(
    materialized="incremental",
    unique_key=['sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(big="XL")
) }}

-- read observed and imputed five minutes data
with obs_imputed_five_minutes_agg as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where station_type in ('HV', 'ML') and {{ make_model_incremental('sample_date') }}
),

imputation_status_count as (
    select
        sample_date,
        count(*) as sample_ct,
        count_if(occupancy_imputation_method = 'local') as occ_local_imputation_sample,
        count_if(occupancy_imputation_method = 'regional') as occ_regional_imputation_sample,
        count_if(occupancy_imputation_method = 'global') as occ_global_imputation_sample,
        count_if(occupancy_imputation_method = 'local_avg') as occ_local_avg_imputation_sample,
        count_if(occupancy_imputation_method = 'regional_avg') as occ_regional_avg_imputation_sample,
        count_if(occupancy_imputation_method = 'observed') as occ_observed_sample,
        count_if(occupancy_imputation_method is NULL) as occ_unobserved_unimputed,
        count_if(volume_imputation_method = 'local') as vol_local_imputation_sample,
        count_if(volume_imputation_method = 'regional') as vol_regional_imputation_sample,
        count_if(volume_imputation_method = 'global') as vol_global_imputation_sample,
        count_if(volume_imputation_method = 'local_avg') as vol_local_avg_imputation_sample,
        count_if(volume_imputation_method = 'regional_avg') as vol_regional_avg_imputation_sample,
        count_if(volume_imputation_method = 'observed') as vol_observed_sample,
        count_if(volume_imputation_method is NULL) as vol_unobserved_unimputed,
        count_if(speed_imputation_method = 'local') as speed_local_imputation_sample,
        count_if(speed_imputation_method = 'regional') as speed_regional_imputation_sample,
        count_if(speed_imputation_method = 'global') as speed_global_imputation_sample,
        count_if(speed_imputation_method = 'local_avg') as speed_local_avg_imputation_sample,
        count_if(speed_imputation_method = 'regional_avg') as speed_regional_avg_imputation_sample,
        count_if(speed_imputation_method = 'observed') as speed_observed_sample,
        count_if(speed_imputation_method is NULL) as speed_unobserved_unimputed,
        count_if(occupancy_imputation_method != 'observed' and occupancy_imputation_method is not NULL)
            as occ_imputed_sample,
        count_if(volume_imputation_method != 'observed' and volume_imputation_method is not NULL) as vol_imputed_sample,
        count_if(speed_imputation_method != 'observed' and speed_imputation_method is not NULL) as speed_imputed_sample
    from obs_imputed_five_minutes_agg
    group by sample_date
),

sample_count as (
    select
        *,
        (vol_imputed_sample / nullif(sample_ct, 0)) * 100 as pct_vol_imputed,
        (vol_observed_sample / nullif(sample_ct, 0)) * 100 as pct_vol_observed,
        (vol_unobserved_unimputed / nullif(sample_ct, 0)) * 100 as pct_vol_unobserved,
        (speed_imputed_sample / nullif(sample_ct, 0)) * 100 as pct_speed_imputed,
        (speed_observed_sample / nullif(sample_ct, 0)) * 100 as pct_speed_observed,
        (speed_unobserved_unimputed / nullif(sample_ct, 0)) * 100 as pct_speed_unobserved,
        (occ_imputed_sample / nullif(sample_ct, 0)) * 100 as pct_occ_imputed,
        (occ_observed_sample / nullif(sample_ct, 0)) * 100 as pct_occ_observed,
        (occ_unobserved_unimputed / nullif(sample_ct, 0)) * 100 as pct_occ_unobserved,

        -- Volume check: Sum of all volume percentages should equal 100
        coalesce(
            coalesce(pct_vol_imputed, 0) + coalesce(pct_vol_observed, 0) + coalesce(pct_vol_unobserved, 0) = 100,
            FALSE
        ) as volume_check_ok,

        -- Speed check: Sum of all speed percentages should equal 100
        coalesce(
            coalesce(pct_speed_imputed, 0) + coalesce(pct_speed_observed, 0) + coalesce(pct_speed_unobserved, 0)
            = 100,
            FALSE
        ) as speed_check_ok,

        -- Occupancy check: Sum of all occupancy percentages should equal 100
        coalesce(
            coalesce(pct_occ_imputed, 0) + coalesce(pct_occ_observed, 0) + coalesce(pct_occ_unobserved, 0) = 100,
            FALSE
        ) as occ_check_ok

    from imputation_status_count
)

select *
from sample_count

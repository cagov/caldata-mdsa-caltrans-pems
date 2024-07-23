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
        direction,
        lane,
        freeway,
        count(station_id) as sample_ct,
        count(case when occupancy_imputation_method = 'local' then station_id end) as occ_local_imputation_sample,
        count(case when occupancy_imputation_method = 'regional' then station_id end) as occ_regional_imputation_sample,
        count(case when occupancy_imputation_method = 'global' then station_id end) as occ_global_imputation_sample,
        count(case when occupancy_imputation_method = 'observed' then station_id end) as occ_observed_sample,
        count(case when occupancy_imputation_method is NULL then station_id end) as occ_unobserved_unimputed,
        count(case when volume_imputation_method = 'local' then station_id end) as vol_local_imputation_sample,
        count(case when volume_imputation_method = 'regional' then station_id end) as vol_regional_imputation_sample,
        count(case when volume_imputation_method = 'global' then station_id end) as vol_global_imputation_sample,
        count(case when volume_imputation_method = 'observed' then station_id end) as vol_observed_sample,
        count(case when volume_imputation_method is NULL then station_id end) as vol_unobserved_unimputed,
        count(case when speed_imputation_method = 'local' then station_id end) as speed_local_imputation_sample,
        count(case when speed_imputation_method = 'regional' then station_id end) as speed_regional_imputation_sample,
        count(case when speed_imputation_method = 'global' then station_id end) as speed_global_imputation_sample,
        count(case when speed_imputation_method = 'observed' then station_id end) as speed_observed_sample,
        count(case when speed_imputation_method is NULL then station_id end) as speed_unobserved_unimputed
    from obs_imputed_five_minutes_agg
    group by station_id, district, lane, direction, freeway, sample_date
),

imputation_pct as (
    select
        station_id,
        district,
        sample_date,
        direction,
        lane,
        freeway,
        sample_ct,
        (coalesce(occ_local_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_occupancy_local_regression,
        (coalesce(occ_regional_imputation_sample, 0) / nullif(sample_ct, 0))
        * 100 as pct_of_occupancy_regional_regression,
        (coalesce(occ_global_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_occupancy_global_regression,
        (coalesce(occ_unobserved_unimputed, 0) / nullif(sample_ct, 0)) * 100 as pct_of_occupancy_unobserved_unimputed,
        (coalesce(occ_observed_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_occupancy_observed,
        (coalesce(vol_local_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_volume_local_regression,
        (coalesce(vol_regional_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_volume_regional_regression,
        (coalesce(vol_global_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_volume_global_regression,
        (coalesce(vol_unobserved_unimputed, 0) / nullif(sample_ct, 0)) * 100 as pct_of_volume_unobserved_unimputed,
        (coalesce(vol_observed_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_volume_observed,
        (coalesce(speed_local_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_speed_local_regression,
        (coalesce(speed_regional_imputation_sample, 0) / nullif(sample_ct, 0))
        * 100 as pct_of_speed_regional_regression,
        (coalesce(speed_global_imputation_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_speed_global_regression,
        (coalesce(speed_unobserved_unimputed, 0) / nullif(sample_ct, 0)) * 100 as pct_of_speed_unobserved_unimputed,
        (coalesce(speed_observed_sample, 0) / nullif(sample_ct, 0)) * 100 as pct_of_speed_observed
    from imputation_count
)

select * from imputation_pct

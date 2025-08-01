{{ config(
    materialized="incremental",
    unique_key=['detector_id', 'sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(big="XL"),
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
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
        detector_id,
        lane,
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
        count_if(speed_imputation_method is NULL) as speed_unobserved_unimputed
    from obs_imputed_five_minutes_agg
    group by detector_id, sample_date, station_id, lane
),

imputation_pct as (
    select
        detector_id,
        station_id,
        lane,
        sample_date,
        sample_ct,
        coalesce(occ_local_imputation_sample, 0) / nullifzero(sample_ct)
        * 100 as pct_of_occupancy_local_regression,
        coalesce(occ_regional_imputation_sample, 0) / nullifzero(sample_ct)
        * 100 as pct_of_occupancy_regional_regression,
        coalesce(occ_global_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_occupancy_global_regression,
        coalesce(occ_local_avg_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_occupancy_local_avg,
        coalesce(occ_regional_avg_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_occupancy_regional_avg,
        coalesce(occ_unobserved_unimputed, 0) / nullifzero(sample_ct) * 100 as pct_of_occupancy_unobserved_unimputed,
        coalesce(occ_observed_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_occupancy_observed,

        coalesce(vol_local_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_local_regression,
        coalesce(vol_regional_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_regional_regression,
        coalesce(vol_global_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_global_regression,
        coalesce(vol_local_avg_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_local_avg,
        coalesce(vol_regional_avg_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_regional_avg,
        coalesce(vol_unobserved_unimputed, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_unobserved_unimputed,
        coalesce(vol_observed_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_volume_observed,

        coalesce(speed_local_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_speed_local_regression,
        coalesce(speed_regional_imputation_sample, 0) / nullifzero(sample_ct)
        * 100 as pct_of_speed_regional_regression,
        coalesce(speed_global_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_speed_global_regression,
        coalesce(speed_local_avg_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_speed_local_avg,
        coalesce(speed_regional_avg_imputation_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_speed_regional_avg,
        coalesce(speed_unobserved_unimputed, 0) / nullifzero(sample_ct) * 100 as pct_of_speed_unobserved_unimputed,
        coalesce(speed_observed_sample, 0) / nullifzero(sample_ct) * 100 as pct_of_speed_observed
    from imputation_count
)

select * from imputation_pct

{{ config(
        materialized='incremental',
        on_schema_change="append_new_columns",
        cluster_by=["sample_date"],
        unique_key=["detector_id", "sample_timestamp", "sample_date"],
        snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL", small="XS"),
    )
}}

-- read observed and imputed five minutes data
with obs_imputed_five_minutes_agg as (
    select *
    from {{ ref('int_imputation__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

-- now select the final speed, volume and occupancy
-- tag if it is observed or imputed
hybrid_five_mins_agg as (
    select
        station_id,
        detector_id,
        station_type,
        lane,
        direction,
        county,
        city,
        district,
        freeway,
        length,
        detector_is_good,
        sample_date,
        sample_timestamp,
        absolute_postmile,
        sample_ct,
        station_valid_from,
        station_valid_to,
        -- select the imputed value
        case
            when detector_is_good = false or volume_sum is null
                then
                    coalesce(
                        volume_local_regression,
                        volume_regional_regression,
                        volume_global_regression,
                        volume_local_avg,
                        volume_regional_avg
                    )
            else volume_sum
        end as volume_sum,
        case
            when detector_is_good = false or speed_five_mins is null
                then
                    coalesce(
                        speed_local_regression,
                        speed_regional_regression,
                        speed_global_regression,
                        speed_local_avg,
                        speed_regional_avg
                    )
            else speed_five_mins
        end as speed_five_mins,
        case
            when detector_is_good = false or occupancy_avg is null
                then
                    coalesce(
                        occupancy_local_regression,
                        occupancy_regional_regression,
                        occupancy_global_regression,
                        occupancy_local_avg,
                        occupancy_regional_avg
                    )
            else occupancy_avg
        end as occupancy_avg,
        -- assign the imputation date
        case
            when
                detector_is_good = false
                or volume_sum is null
                or occupancy_avg is null
                or speed_five_mins is null
                then
                    coalesce(local_regression_date, regional_regression_date, global_regression_date)
            else sample_date
        end as regression_date,

        -- assign the imputation method
        case
            when
                (detector_is_good = false or volume_sum is null) and volume_local_regression is not null
                then 'local'
            when
                (detector_is_good = false or volume_sum is null) and volume_regional_regression is not null
                then 'regional'
            when
                (detector_is_good = false or volume_sum is null) and volume_global_regression is not null
                then 'global'
            when
                (detector_is_good = false or volume_sum is null) and volume_local_avg is not null
                then 'local_avg'
            when
                (detector_is_good = false or volume_sum is null) and volume_regional_avg is not null
                then 'regional_avg'
            when
                (detector_is_good = false or volume_sum is null)
                and volume_local_regression is null
                and volume_regional_regression is null
                and volume_global_regression is null
                and volume_local_avg is null
                and volume_regional_avg is null
                then 'observed_unimputed'
            else 'observed'
        end as volume_imputation_method,
        case
            when
                (detector_is_good = false or speed_five_mins is null) and speed_local_regression is not null
                then 'local'
            when
                (detector_is_good = false or speed_five_mins is null) and speed_regional_regression is not null
                then 'regional'
            when
                (detector_is_good = false or speed_five_mins is null) and speed_global_regression is not null
                then 'global'
            when
                (detector_is_good = false or speed_five_mins is null) and speed_local_avg is not null
                then 'local_avg'
            when
                (detector_is_good = false or speed_five_mins is null) and speed_regional_avg is not null
                then 'regional_avg'
            when
                (detector_is_good = false or speed_five_mins is null)
                and speed_local_regression is null
                and speed_regional_regression is null
                and speed_global_regression is null
                and speed_local_avg is null
                and speed_regional_avg is null
                then 'observed_unimputed'
            else 'observed'
        end as speed_imputation_method,
        case
            when
                (detector_is_good = false or occupancy_avg is null) and occupancy_local_regression is not null
                then 'local'
            when
                (detector_is_good = false or occupancy_avg is null) and occupancy_regional_regression is not null
                then 'regional'
            when
                (detector_is_good = false or occupancy_avg is null) and occupancy_global_regression is not null
                then 'global'
            when
                (detector_is_good = false or occupancy_avg is null) and occupancy_local_avg is not null
                then 'local_avg'
            when
                (detector_is_good = false or occupancy_avg is null) and occupancy_regional_avg is not null
                then 'regional_avg'
            when
                (detector_is_good = false or occupancy_avg is null)
                and occupancy_local_regression is null
                and occupancy_regional_regression is null
                and occupancy_global_regression is null
                and occupancy_local_avg is null
                and occupancy_regional_avg is null
                then 'observed_unimputed'
            else 'observed'
        end as occupancy_imputation_method
    from obs_imputed_five_minutes_agg
)

-- select the final observed and imputed five mins agg table
select * from hybrid_five_mins_agg

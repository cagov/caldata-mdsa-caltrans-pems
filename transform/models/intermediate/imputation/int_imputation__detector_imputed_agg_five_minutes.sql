{{ config(
        materialized='incremental',
        on_schema_change="append_new_columns",
        cluster_by=["sample_date"],
        unique_key=["station_id", "lane", "sample_timestamp", "sample_date"],
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
        detector_is_good,
        sample_date,
        sample_timestamp,
        absolute_postmile,
        sample_ct,
        -- select the imputed value
        case
            when detector_is_good = false
                then
                    coalesce(volume_local_regression, volume_regional_regression, volume_global_regression)
            else volume_sum
        end as volume_sum,
        case
            when detector_is_good = false
                then
                    coalesce(speed_local_regression, speed_regional_regression, speed_global_regression)
            else speed_five_mins
        end as speed_five_mins,
        case
            when detector_is_good = false
                then
                    coalesce(
                        occupancy_local_regression,
                        occupancy_regional_regression,
                        occupancy_global_regression
                    )
            else occupancy_avg
        end as occupancy_avg,
        -- assign the imputation date
        case
            when detector_is_good = false
                then
                    coalesce(local_regression_date, regional_regression_date, global_regression_date)
            else sample_date
        end as regression_date,

        -- assign the imputation method
        case
            when
                detector_is_good = false and volume_local_regression is not null
                then 'local'
            when
                detector_is_good = false and volume_regional_regression is not null
                then 'regional'
            when
                detector_is_good = false and volume_global_regression is not null
                then 'global'
            else 'observed'
        end as volume_imputation_method,
        case
            when
                detector_is_good = false and speed_local_regression is not null
                then 'local'
            when
                detector_is_good = false and speed_regional_regression is not null
                then 'regional'
            when
                detector_is_good = false and speed_global_regression is not null
                then 'global'
            else 'observed'
        end as speed_imputation_method,
        case
            when
                detector_is_good = false and occupancy_local_regression is not null
                then 'local'
            when
                detector_is_good = false and occupancy_regional_regression is not null
                then 'regional'
            when
                detector_is_good = false and occupancy_global_regression is not null
                then 'global'
            else 'observed'
        end as occupancy_imputation_method
    from obs_imputed_five_minutes_agg
)

-- select the final observed and imputed five mins agg table
select * from hybrid_five_mins_agg

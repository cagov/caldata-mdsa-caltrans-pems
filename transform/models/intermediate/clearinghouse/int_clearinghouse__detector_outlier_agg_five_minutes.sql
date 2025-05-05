{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    cluster_by=["sample_date"],
    snowflake_warehouse = "TRANSFORMING_L_DEV",
) }}

-- retrieve recent five-minute data
with five_minute_agg as (
    select *
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
),

thresholds as (
    select * from {{ ref('int_diagnostics__detector_outlier_thresholds') }}
),

-- impute detected outliers
outlier_removed_data as (
    select
        fa.*,
        -- update volume_sum if it's an outlier
        case
            when
                (fa.volume_sum - thresholds.volume_mean) / nullifzero(thresholds.volume_stddev) > 3
                then thresholds.volume_95th
            else fa.volume_sum
        end as updated_volume_sum,
        -- add a volume_label for imputed volume
        case
            when
                (fa.volume_sum - thresholds.volume_mean) / nullifzero(thresholds.volume_stddev) > 3
                then 'observed outlier'
            else 'observed data'
        end as volume_label,
        -- update occupancy if it's an outlier
        case
            when
                fa.occupancy_avg > thresholds.occupancy_95th
                then thresholds.occupancy_95th
            else fa.occupancy_avg
        end as updated_occupancy_avg,
        -- add a column for imputed occupancy
        case
            when
                fa.occupancy_avg > thresholds.occupancy_95th
                then 'observed outlier'
            else 'observed data'
        end as occupancy_label
    from five_minute_agg as fa
    asof join thresholds
        match_condition (fa.sample_date >= thresholds.agg_date)
        on
            fa.detector_id = thresholds.detector_id
)

select * from outlier_removed_data

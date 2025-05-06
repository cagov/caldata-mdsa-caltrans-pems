{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="sample_date",
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

/* Get date range where a detector is expected to be collecting data. */
detector_date_range as (
    select *
    from {{ ref('int_vds__active_detectors') }}
    where
        active_date >= '{{ model.batch.event_time_start if model.batch else "2000-01-01" }}'
        and active_date < '{{ model.batch.event_time_end if model.batch else "2001-01-01" }}'
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
),

timestamp_spine as (
    {{ timestamp_spine(
        start_date=var('pems_clearinghouse_start_date'),
        end_date="current_date()",
        second_increment=60*5
    ) }}
),

/* Expand timestamp spine to include values per detector but only for days within the detector's date range */
spine as (
    select
        timestamp_spine.timestamp_column as sample_timestamp,
        detector_date_range.active_date as sample_date,
        detector_date_range.* exclude (active_date)
    from timestamp_spine
    inner join detector_date_range
        on to_date(timestamp_spine.timestamp_column) = detector_date_range.active_date
),

/* Join 5-minute aggregated data to the spine to get a table without missing rows */
base as (
    select
        spine.detector_id,
        spine.sample_timestamp,
        spine.sample_date,
        spine.station_id,
        spine.district,
        spine.lane,
        agg.sample_ct,
        agg.volume_sum,
        agg.zero_vol_ct,
        agg.occupancy_avg,
        agg.zero_occ_ct,
        agg.zero_vol_pos_occ_ct,
        agg.zero_occ_pos_vol_ct,
        agg.high_volume_ct,
        agg.high_occupancy_ct,
        agg.speed_weighted,
        agg.volume_observed,
        spine.state_postmile,
        spine.absolute_postmile,
        spine.latitude,
        spine.longitude,
        spine.physical_lanes,
        spine.station_type,
        spine.county,
        spine.city,
        spine.freeway,
        spine.direction,
        spine.length
    from spine
    left join outlier_removed_data as agg
        on spine.sample_timestamp = agg.sample_timestamp and spine.detector_id = agg.detector_id
)

select * from base

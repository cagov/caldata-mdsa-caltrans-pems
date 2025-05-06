{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    cluster_by=["sample_date"],
    snowflake_warehouse = "TRANSFORMING_L_DEV"
) }}

with timestamp_spine as (
    {{ timestamp_spine(
        start_date=var('pems_clearinghouse_start_date'),
        end_date="current_date()",
        second_increment=60*5
    ) }}
),

detector_agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes_normalized') }}
),

/* Get date range where a detector is expected to be collecting data. */
detector_date_range as (
    select *
    from {{ ref('int_vds__active_detectors') }}
    where
        active_date >= '{{ model.batch.event_time_start if model.batch else "2000-01-01" }}'
        and active_date < '{{ model.batch.event_time_end if model.batch else "2001-01-01" }}'
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
    left join detector_agg as agg
        on spine.sample_timestamp = agg.sample_timestamp and spine.detector_id = agg.detector_id
)

select * from base

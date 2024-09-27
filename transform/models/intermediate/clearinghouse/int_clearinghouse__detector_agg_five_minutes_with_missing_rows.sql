{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["detector_id", "sample_timestamp","sample_date"],
    on_schema_change="append_new_columns",
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL", big="XL")
) }}

with timestamp_spine as (
    {{ timestamp_spine(start_date="'2023-01-01'",
        end_date="current_date()",
        second_increment=60*5
    ) }}
),

detector_agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

detector_meta as (
    select * from {{ ref('int_vds__detector_config') }}
),

/*
 * Get date range where a detector is expected to be collecting data.
 * This could pull from the active stations model instead?
 */
detector_date_range as (
    select
        detector_id,
        active_date
    from {{ ref('int_vds__active_detectors') }}
),

/* Expand timestamp spine to include values per detector but only for days within the detector's date range */
spine as (
    select
        ts.timestamp_column,
        dd.detector_id
    from timestamp_spine as ts inner join detector_date_range as dd
        on to_date(ts.timestamp_column) = dd.active_date
),

/* Join 5-minute aggregated data to the spine to get a table without missing rows */
base as (
    select
        spine.detector_id,
        coalesce(agg.sample_date, to_date(spine.timestamp_column)) as sample_date,
        coalesce(agg.sample_timestamp, spine.timestamp_column) as sample_timestamp,
        coalesce(agg.station_id, dmeta.station_id) as station_id,
        coalesce(agg.district, dmeta.district) as district,
        agg.sample_ct,
        coalesce(agg.lane, dmeta.lane) as lane,
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
        coalesce(agg.state_postmile, dmeta.state_postmile) as state_postmile,
        coalesce(agg.absolute_postmile, dmeta.absolute_postmile) as absolute_postmile,
        coalesce(agg.latitude, dmeta.latitude) as latitude,
        coalesce(agg.longitude, dmeta.longitude) as longitude,
        coalesce(agg.physical_lanes, dmeta.physical_lanes) as physical_lanes,
        coalesce(agg.station_type, dmeta.station_type) as station_type,
        coalesce(agg.county, dmeta.county) as county,
        coalesce(agg.city, dmeta.city) as city,
        coalesce(agg.freeway, dmeta.freeway) as freeway,
        coalesce(agg.direction, dmeta.direction) as direction,
        coalesce(agg.length, dmeta.length) as length,
        coalesce(agg.station_valid_from, dmeta._valid_from) as station_valid_from,
        coalesce(agg.station_valid_to, dmeta._valid_to) as station_valid_to
    from spine
    left join detector_agg as agg
        on spine.timestamp_column = agg.sample_timestamp and spine.detector_id = agg.detector_id

    -- The previously "missing" rows will need metadata filled in
    left join detector_meta as dmeta
        on
            agg.sample_ct is null -- this filters for missing rows since it is a computed value in upstream models
            and spine.detector_id = dmeta.detector_id
            and to_date(spine.timestamp_column) >= dmeta._valid_from
            and (
                to_date(spine.timestamp_column) < dmeta._valid_to
                or dmeta._valid_to is null
            )
)

select * from base

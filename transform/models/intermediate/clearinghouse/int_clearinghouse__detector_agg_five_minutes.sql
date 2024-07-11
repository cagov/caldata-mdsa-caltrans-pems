{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["detector_id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with station_raw as (
    select
        id as station_id,
        district,
        sample_date,
        sample_timestamp,
        lane,
        volume,
        occupancy,
        speed,
        /* Create a timestamp truncated down to the nearest five
         minute bucket. This will be the the timestamp on which
         we aggregate. If a 30-second interval straddles two different
         buckets, it will be assigned to the one latter one due to
         the floor() call.
        */
        dateadd(
            'minute',
            floor(minute(sample_timestamp) / 5) * 5,
            trunc(sample_timestamp, 'hour')
        ) as sample_timestamp_trunc
    from {{ ref('stg_clearinghouse__station_raw') }}
    where {{ make_model_incremental('sample_date') }}
),

aggregated_speed as (
    select
        station_id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        lane,
        --Number of raw data samples
        count_if(volume is not null and occupancy is not null)
            as sample_ct,
        -- Sum of all the flow values
        sum(volume) as volume_sum,
        -- Average of all the occupancy values
        avg(occupancy) as occupancy_avg,
        -- calculate_weighted_speed
        sum(volume * speed) / nullifzero(sum(volume)) as speed_weighted
    from station_raw
    group by station_id, lane, sample_date, sample_timestamp_trunc
),

aggregated_speed_with_metadata as (
    select
        agg.station_id,
        dmeta.detector_id,
        agg.sample_date,
        agg.sample_timestamp,
        agg.lane,
        agg.sample_ct,
        agg.volume_sum,
        agg.occupancy_avg,
        agg.speed_weighted,
        dmeta.state_postmile,
        dmeta.absolute_postmile,
        dmeta.latitude,
        dmeta.longitude,
        dmeta.physical_lanes,
        dmeta.station_type,
        dmeta.district,
        dmeta.county,
        dmeta.city,
        dmeta.freeway,
        dmeta.direction,
        dmeta.length,
        dmeta._valid_from as station_valid_from,
        dmeta._valid_to as station_valid_to
    from aggregated_speed as agg inner join {{ ref('int_vds__detector_config') }} as dmeta
        on
            agg.station_id = dmeta.station_id
            and agg.lane = dmeta.lane
            and agg.sample_date >= dmeta._valid_from
            and (
                agg.sample_date < dmeta._valid_to
                or dmeta._valid_to is null
            )
)

select * from aggregated_speed_with_metadata

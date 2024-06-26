{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}

with detector_config as (
    select * from {{ ref('int_vds__detector_config') }}
),

aggregated_speed as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

/*with station_raw as (
    select
        *,
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
        id as station_id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        lane,
        district,
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
    group by id, lane, sample_date, sample_timestamp_trunc, district
)*/

aggregated_speed_with_config as (
    select
        aggregated_speed.*,
        detector_config.* exclude (station_id, lane, district)
    from aggregated_speed
    left join detector_config
        on
            aggregated_speed.id = detector_config.station_id
            and aggregated_speed.lane = detector_config.lane
            and aggregated_speed.sample_date >= detector_config._valid_from
            and (aggregated_speed.sample_date < detector_config._valid_to or detector_config._valid_to is null)
)

select * from aggregated_speed_with_config

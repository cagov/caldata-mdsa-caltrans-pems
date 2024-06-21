{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with station_raw as (
    select
        *,
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
    {{ make_model_incremental('sample_date') }}
),

aggregated_speed as (
    select
        id,
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
)

select * from aggregated_speed

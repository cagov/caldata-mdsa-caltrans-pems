{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["ID", "SAMPLE_TIMESTAMP"],
    snowflake_warehouse=get_snowflake_refresh_warehouse()
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
    {% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where sample_date > (
            select dateadd(day, -1, max(sample_date)) from {{ this }}
        )
    {% endif %}
),

aggregated as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        -- Sum of all the flow values
        sum(flow_1) as flow_1,
        sum(flow_2) as flow_2,
        sum(flow_3) as flow_3,
        sum(flow_4) as flow_4,
        sum(flow_5) as flow_5,
        sum(flow_6) as flow_6,
        sum(flow_7) as flow_7,
        sum(flow_8) as flow_8,
        -- Average of all the occupancy values
        avg(occupancy_1) as occupancy_1,
        avg(occupancy_2) as occupancy_2,
        avg(occupancy_3) as occupancy_3,
        avg(occupancy_4) as occupancy_4,
        avg(occupancy_5) as occupancy_5,
        avg(occupancy_6) as occupancy_6,
        avg(occupancy_7) as occupancy_7,
        avg(occupancy_8) as occupancy_8,
        -- Average of all the speed values
        avg(speed_1) as speed_1,
        avg(speed_2) as speed_2,
        avg(speed_3) as speed_3,
        avg(speed_4) as speed_4,
        avg(speed_5) as speed_5,
        avg(speed_6) as speed_6,
        avg(speed_7) as speed_7,
        avg(speed_8) as speed_8
    from station_raw
    group by id, sample_date, sample_timestamp_trunc
)

select * from aggregated

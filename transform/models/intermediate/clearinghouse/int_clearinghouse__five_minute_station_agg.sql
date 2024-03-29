{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["ID", "LANE", "SAMPLE_TIMESTAMP"],
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
            select dateadd(day, -2, max(sample_date)) from {{ this }}
        )
    {% endif %}
),

aggregated as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        lane,
        sum(volume) as volume, -- Sum of all the flow values
        avg(occupancy) as occupancy -- Average of all the occupancy values
    from station_raw
    group by id, lane, sample_date, sample_timestamp_trunc
)

select * from aggregated

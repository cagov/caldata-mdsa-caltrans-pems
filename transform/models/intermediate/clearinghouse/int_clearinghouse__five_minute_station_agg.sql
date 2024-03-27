{{ config(
    materialized="table",
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
    -- Looking at one day only for testing/validation
    where sample_date = '2023-03-26'--dateadd(year, -1, current_date())
    /*{% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where sample_date > (
            select dateadd(day, -2, max(sample_date)) from {{ this }}
        )
    {% endif %}
    */
),

aggregated as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        lane,
        sum(volume) as volume, -- Sum of all the flow values
        avg(occupancy) as occupancy, -- Average of all the occupancy values
        max(occupancy) as max_occupancy -- Maximum of all the occupancy values
    from station_raw
    group by id, lane, sample_date, sample_timestamp_trunc
),

aggregated_speed as (
    select
        *,
        --For speed I used the following formula to get speed: 
        --SPEED = SUM(FLOW)/AVG(OCCUPANCY)/600 which resulted in 
        --values from 0 to 5
        --On 3/22/24 I updated the formula to use a vehicle effective length
        --of 22 feet (16 ft vehicle + 6 ft detector zone) feet and using 
        --a conversion to get miles per hour (5280 ft / mile and 12
        --5-minute intervals in an hour).
        case
            when volume = 0 or occupancy = 0 then 0
            when volume is null or occupancy is null then null
            else (volume * 22) / max_occupancy * (1 / 5280) * 12
            --else volume / occupancy / 600
        end as speed
    from aggregated
)

select * from aggregated_speed

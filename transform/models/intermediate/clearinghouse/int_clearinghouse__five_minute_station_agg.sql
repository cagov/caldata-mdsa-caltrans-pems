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
    {% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where
            sample_date > (
                select
                    dateadd(
                        day,
                        {{ var("incremental_model_look_back") }},
                        max(sample_date)
                    )
                from {{ this }}
            )
            {% if target.name != 'prd' %}
                and sample_date
                >= dateadd(
                    day,
                    {{ var("dev_model_look_back") }},
                    current_date()
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where sample_date >= dateadd(day, {{ var("dev_model_look_back") }}, current_date())
    {% endif %}
),

aggregated as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        lane,
        --Number of raw data samples
        count_if(volume is not null and occupancy is not null)
            as sample_ct,
        -- Sum of all the flow values 
        sum(volume) as volume_five_mins,
        -- Average of all the occupancy values 
        avg(occupancy) as occupancy_five_mins,
        -- calculate_weighted_speed
        sum(volume * speed) / nullifzero(sum(volume)) as speed_wt_five_mins
    from station_raw
    group by id, lane, sample_date, sample_timestamp_trunc
),

aggregated_speed as (
    select
        *,
        --A preliminary speed calcuation was developed on 3/22/24
        --using a vehicle effective length of 22 feet
        --(16 ft vehicle + 6 ft detector zone) feet and using
        --a conversion to get miles per hour (5280 ft / mile and 12
        --5-minute intervals in an hour).
        --The following code may be used if we want to use speed from raw data
        --coalesce(speed_raw, ((volume * 22) / nullifzero(occupancy)
        --* (1 / 5280) * 12))
        --impute five minutes missing speed
        coalesce(speed_wt_five_mins, (volume_five_mins * 22) / nullifzero(occupancy_five_mins) * (1 / 5280) * 12)
            as speed_five_mins
    from aggregated
)

select * from aggregated_speed

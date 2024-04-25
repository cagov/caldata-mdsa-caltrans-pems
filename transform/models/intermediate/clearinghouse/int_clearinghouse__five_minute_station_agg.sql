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
        -- Number of raw data samples
        count_if(volume is not null and occupancy is not null)
            as sample_ct,
        -- Sum of all the flow values
        sum(volume) as volume_sum,
        -- Average of all the occupancy values
        avg(occupancy) as average_occupancy,
        -- weighted speed provides more accurate aggregation than average speed, therefore we will weighted speed
        sum(volume * speed) / nullifzero(sum(volume)) as weighted_speed
    from station_raw
    group by id, lane, sample_date, sample_timestamp_trunc
),

aggregated_metrics as (
    select
        *,
        case
            when
                -- average occupancy should not be null and zero
                -- sum of the volume should not be null and zero
                (average_occupancy is not null and average_occupancy != 0)
                and (volume_sum is not null and volume_sum != 0)
                then
                    weighted_speed
        end as five_mins_speed
    from aggregated
)

select * from aggregated_metrics

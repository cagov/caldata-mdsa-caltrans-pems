{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['id', 'lane', 'sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with

source as (
    select * from {{ ref("int_clearinghouse__five_minute_station_agg") }}
    where
        TO_TIME(sample_timestamp) >= {{ var("day_start") }}
        and TO_TIME(sample_timestamp) <= {{ var("day_end") }}
        {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select
                    DATEADD(
                        day,
                        {{ var("incremental_model_look_back") }},
                        MAX(sample_date)
                    )
                from {{ this }}
            )
        {% endif %}
        {% if target.name != 'prd' %}
            and sample_date
            >= DATEADD('day', {{ var("dev_model_look_back") }}, CURRENT_DATE())
        {% endif %}
),

calculate_occupancy_delta as (
    select
        id,
        sample_timestamp,
        sample_date,
        lane,
        occupancy,
        occupancy
        - LAG(occupancy)
            over (partition by id, lane, sample_date order by sample_timestamp)
            as occupancy_delta
    from source
),

sum_occupancy_delta as (
    select
        *,
        ABS(occupancy_delta) as abs_val_occupancy_delta,
        SUM(abs_val_occupancy_delta)
        /* we are looking at a window of 48 rows because that is a 4 hour window
        (5 min data * 12 = 60 (one hour) then 12 * 4 = 48 which is 4 hours) */
            over (
                partition by id, lane, sample_date
                order by sample_timestamp rows between 47 preceding and current row
            )
            as abs_val_occupancy_delta_summed
    from calculate_occupancy_delta
    qualify occupancy != 0 or occupancy is not null
)

select
    id,
    sample_date,
    lane,
    MIN(abs_val_occupancy_delta_summed) as min_occupancy_delta
from sum_occupancy_delta
group by id, lane, sample_date
order by sample_date

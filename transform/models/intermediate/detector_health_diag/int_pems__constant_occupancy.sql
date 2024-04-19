{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['id', 'sample_date'],
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
    order by sample_timestamp
),

sum_occupancy_delta as (
    select
        *,
        ABS(occupancy_delta) as abs_val_occupancy_delta,
        SUM(abs_val_occupancy_delta)
            over (
                partition by id, lane, sample_date
                order by sample_timestamp rows between 47 preceding and current row
            )
            as abs_val_occupancy_delta_summed
    from calculate_occupancy_delta
    order by sample_timestamp
)

select
    id,
    sample_date,
    lane,
    MIN(abs_val_occupancy_delta_summed) as min_delta_occupancy
from sum_occupancy_delta
where occupancy != 0 or occupancy is not null
group by id, lane, sample_date
order by sample_date
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
                    DATEADD(day, {{ var("incremental_model_look_back") }}, MAX(sample_date))
                from {{ this }}
            )
        {% endif %}
        {% if target.name != 'prd' %}
            and sample_date
            >= DATEADD('day', {{ var("dev_model_look_back") }}, CURRENT_DATE())
        {% endif %}
),

calculate_occupancy_difference as (
    select
        id,
        sample_timestamp,
        sample_date,
        occupancy,
        case
            when occupancy != 0
                then occupancy - LAG(occupancy) over (partition by id, sample_timestamp order by sample_timestamp)
        end as occupancy_difference
    from source
    group by id, sample_timestamp, sample_date, occupancy
    order by sample_timestamp

)

select
    *,
    ABS(occupancy_difference) as abs_val_occupancy_difference
from calculate_occupancy_difference
qualify
    SUM(abs_val_occupancy_difference) over (partition by id, sample_timestamp order by sample_timestamp rows between 47 preceding and current row) = 0
order by sample_timestamp

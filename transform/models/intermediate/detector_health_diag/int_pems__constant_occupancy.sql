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
        occupancy - LAG(occupancy) over (partition by id order by sample_timestamp) as occupancy_difference
    from source
    order by sample_timestamp
), 

sum_occupancy_difference as (
select
    *,
    ABS(occupancy_difference) as abs_val_occupancy_difference,
    SUM(abs_val_occupancy_difference) over (partition by id order by sample_timestamp rows between 47 preceding and current row) as abs_val_occupancy_difference_summed
from calculate_occupancy_difference
order by sample_timestamp
)

select
    *,
    case
    when abs_val_occupancy_difference_summed = 0 then true
    else false
    end as constant_occupancy
from sum_occupancy_difference
where occupancy != 0 or occupancy != null 
order by sample_timestamp

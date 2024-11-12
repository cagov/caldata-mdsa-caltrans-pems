{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['detector_id', 'sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with

source as (
    select *
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where
        TO_TIME(sample_timestamp) >= {{ var("day_start") }}
        and TO_TIME(sample_timestamp) <= {{ var("day_end") }}
        and {{ make_model_incremental('sample_date') }}
),


calculate_occupancy_delta as (
    select
        detector_id,
        sample_timestamp,
        sample_date,
        occupancy_avg,
        occupancy_avg
        - LAG(occupancy_avg)
            over (partition by detector_id, sample_date order by sample_timestamp)
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
                partition by detector_id, sample_date
                order by sample_timestamp rows between 47 preceding and current row
            )
            as abs_val_occupancy_delta_summed
    from calculate_occupancy_delta
    qualify
        (occupancy_avg != 0 or occupancy_avg is not null)
        and ROW_NUMBER() over (
            partition by detector_id, sample_date
            order by sample_timestamp
        ) >= 48

)

select
    detector_id,
    sample_date,
    MIN(abs_val_occupancy_delta_summed) as min_occupancy_delta
from sum_occupancy_delta
group by detector_id, sample_date
order by sample_date

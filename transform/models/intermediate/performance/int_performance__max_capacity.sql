{{ config(
    materialized="table"
) }}

with

source as (
    select
        id,
        sample_timestamp,
        sample_date,
        lane,
        volume
    from {{ ref("int_clearinghouse__five_minute_station_agg") }}
),

sum_volume as (
    select
        *,
        SUM(volume)
        /* we are looking at a window of 3 rows because that is a 15-minute window
        (5-min data * 3 = 15 minutes) */
            over (
                partition by id, lane, sample_date
                order by sample_timestamp rows between 2 preceding and current row
            )
            as volume_summed
    from source
    qualify volume is not null
)

select
    id,
    lane,
    /*
    Use max of 2076 v/l/h or 15 minute historical highest flow as the capacity
    at each location per PeMS website:
    https://pems.dot.ca.gov/?dnode=Help&content=help_calc#perf
    Max 15 minute flow / 3 = Max 5 minute flow
    2076 v/l/h / 12 = 173 v/l/5-min
    */
    GREATEST(MAX(volume_summed) / 3, 173) as max_volume_5min
from sum_volume
group by id, lane

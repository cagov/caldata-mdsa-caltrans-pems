/*
This model develops the maximum capacity of a station and lane based on the historical measured
raw data and determines the maximum observed 15-minute flow. We use the maximum of the
actual 15-minute flow value and 2076 v/l/h as the capacity at each location. This will be used
to detemine the productivity performance metric.
*/

with

source as (
    select
        id,
        sample_timestamp,
        sample_date,
        lane,
        volume_sum
    from {{ ref("int_clearinghouse__five_minute_station_agg") }}
),

sum_volume as (
    select
        *,
        SUM(volume_sum)
        /* we are looking at a window of 3 rows because that is a 15-minute window
        (5-min data * 3 = 15 minutes) */
            over (
                partition by id, lane, sample_date
                order by sample_timestamp rows between 2 preceding and current row
            )
            as volume_summed
    from source
    qualify volume_sum is not null
)

select
    id,
    lane,
    /*
    Use max of 2076 v/l/h or 15 minute historical highest flow as the capacity
    at each location per PeMS website:
    https://pems.dot.ca.gov/?dnode=Help&content=help_calc#perf
    2076 v/l/h / 12 = 173 v/l/5-min
    */
    GREATEST(MAX(volume_summed), 173) as max_capacity_5min
from sum_volume
group by id, lane

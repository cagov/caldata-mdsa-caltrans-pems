{{ config(materialized="table") }}
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
        sample_ct,
        volume_sum
    from {{ ref("int_clearinghouse__five_minute_station_agg") }}
),

detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
),

source_with_status as (
    select
        source.*,
        ds.status
    from source
    inner join detector_status as ds
        on
            source.id = ds.station_id
            and source.lane = ds.lane
            and source.sample_date = ds.active_date
),

sum_volume_hour as (
    select
        id,
        lane,
        SUM(volume_sum)
        /* we are looking at a window of 12 rows because that is a 60-minute window
        (5-min data * 12 = 60 minutes) */
            over (
                partition by id, lane, sample_date
                order by sample_timestamp rows between 11 preceding and current row
            )
            as volume_summed_hour
    from source_with_status
    qualify volume_sum is not null and sample_ct >= 10 and status = 'Good'
),

max_volume_hour as (
    select
        id,
        lane,
        MAX(volume_summed_hour) as volume_max_hour
    from sum_volume_hour
    group by id, lane
),

max_volume_5min as (
    select
        id,
        lane,
        MAX(volume_sum) as volume_max_5min
    from source_with_status
    where sample_ct >= 10 and status = 'Good'
    group by id, lane
)

select
    mvf.id,
    mvf.lane,
    /*
    Use max of 2076 v/l/h or hour historical highest flow as the capacity
    for hourly analysus at each location per PeMS website:
    https://pems.dot.ca.gov/?dnode=Help&content=help_calc#perf
    For the 5-minute max capacity analysis use the highest of the
    historical max observed flow or 173 (2076 v/l/h / 12 = 173 v/l/5-min)
    */
    GREATEST(mvf.volume_max_5min, 173) as max_capacity_5min,
    GREATEST(mvh.volume_max_hour, 2076) as max_capacity_hour
from max_volume_5min as mvf
inner join max_volume_hour as mvh
    on
        mvf.id = mvh.id
        and mvf.lane = mvh.lane

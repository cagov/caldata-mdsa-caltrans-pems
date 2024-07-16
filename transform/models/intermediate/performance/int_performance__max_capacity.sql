{{ config(materialized="table") }}
/*
This model develops the maximum capacity of a lane based on analyzing the historical
observed raw data and determines the maximum observed 5-minute, 15 minote and 1-hour
flow. For the hourly capacity we also look at a value of 2076 v/l/h. As an additional
quality control measure we only consider detectors that were evaluated as having a
Good status on the date the flow values are calculated and 10 or more 30 second
samples over a five minute period. These max capacity values will be used to
determine the productivity performance metric and as a check for normalized flow values
at the 5-minute aggregation level by lane.
For the purposes of capacity the current PeMS (July 2024) Systems Calculations website
states that the system looks at the historical measured data for each location and
determines the maximum observed 15-minute flow. PeMS then uses the maximum of this
value and 2076 v/l/h as the capacity at each location.
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
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where sample_ct >= 10
),

detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
    where status = 'Good' and type in ('ML', 'HV')
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
    qualify volume_sum is not null
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
    group by id, lane
),

max_volume_5min_date as (
    select
        sws.id,
        sws.lane,
        MAX(sws.sample_timestamp) as sample_timestamp
    from source_with_status as sws
    inner join max_volume_5min as mvf
        on
            sws.id = mvf.id
            and sws.lane = mvf.lane
            and sws.volume_sum = mvf.volume_max_5min
    group by sws.id, sws.lane
)

select
    mvf.id,
    mvf.lane,
    /*
    Use a max value of 2076 v/l/h as the capacity for hourly analysis.
    For the 5-minute max capacity analysis use 173
    (2076 v/l/h / 12 = 173 v/l/5-min)
    As of 7/16/24 we are seeing max flow values from sensor data for some
    stations that are unrealistic so the Caltrans BIA team is analyzing
    Traffic Census Peak Hour Volume Data found at
    https://dot.ca.gov/programs/traffic-operations/census to determine
    realistic maximum values to be used when looking at a lanes maximum
    capacity.
    */
    mvf.volume_max_5min as max_5min_observed,
    mvfd.sample_timestamp as max_5min_timestamp_observed,
    mvh.volume_max_hour as max_hour_observed,
    173 as max_capacity_5min,
    2076 as max_capacity_hour
from max_volume_5min as mvf
inner join max_volume_hour as mvh
    on
        mvf.id = mvh.id
        and mvf.lane = mvh.lane
inner join max_volume_5min_date as mvfd
    on
        mvf.id = mvfd.id
        and mvf.lane = mvfd.lane

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
For the purposes of capacity the current PeMS July 2024 Systems Calculations website
states that the system looks at the historical measured data for each location and
determines the maximum observed 15-minute flow. PeMS then uses the maximum of this
value and 2076 v/l/h as the capacity at each location.
*/

with

source_samples as (
    select
        station_id,
        sample_timestamp,
        sample_date,
        lane,
        detector_id,
        sample_ct,
        volume_sum
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where sample_ct >= 10
    -- Only use values where all samples are observed
),

detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
    where status = 'Good' and station_type in ('ML', 'HV')
    -- Use Good status stations with specific station types only
),

source_with_status as (
    select
        source_samples.*,
        ds.status
    from source_samples
    inner join detector_status as ds
        on
            source_samples.station_id = ds.station_id
            and source_samples.lane = ds.lane
            and source_samples.sample_date = ds.active_date
),

sum_volume_hour as (
    select
        station_id,
        lane,
        detector_id,
        SUM(volume_sum)
        /* we are looking at a window of 12 rows because that is a 60-minute window
        (5-min data * 12 = 60 minutes) */
            over (
                partition by station_id, lane, sample_date
                order by sample_timestamp rows between 11 preceding and current row
            )
            as volume_summed_hour
    from source_with_status
    qualify volume_sum is not null
),

max_volume_hour as (
    select
        station_id,
        lane,
        detector_id,
        MAX(volume_summed_hour) as volume_max_hour
    from sum_volume_hour
    group by station_id, lane, detector_id
),

max_volume_5min as (
    select
        station_id,
        lane,
        detector_id,
        MAX(volume_sum) as volume_max_5min
    from source_with_status
    group by station_id, lane, detector_id
),

max_volume_5min_date as (
    select
        sws.station_id,
        sws.lane,
        sws.detector_id,
        MAX(sws.sample_timestamp) as sample_timestamp
    from source_with_status as sws
    inner join max_volume_5min as mvf
        on
            sws.station_id = mvf.station_id
            and sws.lane = mvf.lane
            and sws.volume_sum = mvf.volume_max_5min
    group by sws.station_id, sws.lane, sws.detector_id
)

select
    source_samples.station_id,
    source_samples.lane,
    source_samples.detector_id,
    /*
    Use a max value of 2076 v/l/h as the capacity for hourly analysis.
    For the 5-minute max capacity analysis use 173 which represents
    2076 v/l/h / 12 = 173 v/l/5-min.
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
from source_samples
left join max_volume_5min as mvf
    on
        source_samples.station_id = mvf.station_id
        and source_samples.lane = mvf.lane
left join max_volume_hour as mvh
    on
        source_samples.station_id = mvh.station_id
        and source_samples.lane = mvh.lane
left join max_volume_5min_date as mvfd
    on
        source_samples.station_id = mvfd.station_id
        and source_samples.lane = mvfd.lane

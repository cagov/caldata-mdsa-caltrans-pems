{{ config(materialized='table') }}

-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (select * from {{ ref('int_performance__five_min_perform_metrics') }}),

-- separate the hour from the sample_timestamp
hour_extract as (
    select
        id,
        sample_date,
        sample_timestamp,
        lane,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        type,
        extract(hour from sample_timestamp) as sample_hour
    from station_five_mins_data

),

-- now aggregate hourly volume, occupancy and speed
hourly_temporal_metrics as (
    select
        id,
        sample_date,
        lane,
        sample_hour,
        sum(volume_sum) as hourly_volume,
        avg(occupancy_avg) as hourly_occupancy,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as hourly_speed
    from hour_extract
    group by id, sample_date, sample_hour, lane
)

select * from hourly_temporal_metrics

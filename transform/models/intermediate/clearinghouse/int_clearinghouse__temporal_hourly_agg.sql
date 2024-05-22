{{ config(materialized='table') }}

-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        vmt,
        vht,
        delay_35_mph,
        delay_40_mph,
        delay_45_mph,
        delay_50_mph,
        delay_55_mph,
        delay_60_mph,
        extract(hour from sample_timestamp) as sample_hour
    from {{ ref('int_performance__five_min_perform_metrics') }}
),

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_temporal_metrics as (
    select
        id,
        sample_date,
        lane,
        sample_hour,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as hourly_speed,
        sum(vmt) as hourly_vmt,
        sum(vht) as hourly_vht,
        hourly_vmt / nullifzero(hourly_vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti,
        sum(delay_35_mph) as delay_35_mph,
        sum(delay_40_mph) as delay_40_mph,
        sum(delay_45_mph) as delay_45_mph,
        sum(delay_50_mph) as delay_50_mph,
        sum(delay_55_mph) as delay_55_mph,
        sum(delay_60_mph) as delay_60_mph
    from station_five_mins_data
    group by id, sample_date, sample_hour, lane
),

-- read spatial characteristics
hourly_spatial_temporal_metrics as (
    select
        hourly_temporal_metrics.id,
        hourly_temporal_metrics.sample_date,
        hourly_temporal_metrics.sample_hour,
        hourly_temporal_metrics.lane,
        hourly_temporal_metrics.hourly_speed,
        hourly_temporal_metrics.volume_sum,
        hourly_temporal_metrics.occupancy_avg,
        hourly_temporal_metrics.hourly_vmt,
        hourly_temporal_metrics.hourly_vht,
        hourly_temporal_metrics.q_value,
        hourly_temporal_metrics.tti,
        hourly_temporal_metrics.delay_35_mph,
        hourly_temporal_metrics.delay_40_mph,
        hourly_temporal_metrics.delay_45_mph,
        hourly_temporal_metrics.delay_50_mph,
        hourly_temporal_metrics.delay_55_mph,
        hourly_temporal_metrics.delay_60_mph,
        station_meta_data.city,
        station_meta_data.county,
        station_meta_data.district,
        station_meta_data.type
    from {{ ref('int_clearinghouse__most_recent_station_meta') }} as station_meta_data
    inner join hourly_temporal_metrics
        on
            station_meta_data.id = hourly_temporal_metrics.id
            and station_meta_data.lanes = hourly_temporal_metrics.lane
)

select * from hourly_spatial_temporal_metrics

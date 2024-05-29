{{ config(materialized='table') }}
with last_full_month as (
    select
        date_trunc('month', current_date) - interval '1 month' as month_start,
        date_trunc('month', current_date) - interval '1 day' as month_end
),

-- read the volume, occupancy and speed five minutes data
station_five_mins_data as (
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
        lost_productivity_35_mph,
        lost_productivity_40_mph,
        lost_productivity_45_mph,
        lost_productivity_50_mph,
        lost_productivity_55_mph,
        lost_productivity_60_mph,
        date_trunc('hour', sample_timestamp) as sample_timestamp_trunc
    from {{ ref('int_performance__five_min_perform_metrics') }}
    where
        sample_date >= (select month_start from last_full_month)
        and sample_date <= (select month_end from last_full_month)
),

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_temporal_metrics as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_hour,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as hourly_speed,
        sum(vmt) as hourly_vmt,
        sum(vht) as hourly_vht,
        hourly_vmt / nullifzero(hourly_vht) as hourly_q_value,
        -- travel time
        60 / nullifzero(hourly_q_value) as hourly_tti,
        sum(delay_35_mph) as delay_35_mph,
        sum(delay_40_mph) as delay_40_mph,
        sum(delay_45_mph) as delay_45_mph,
        sum(delay_50_mph) as delay_50_mph,
        sum(delay_55_mph) as delay_55_mph,
        sum(delay_60_mph) as delay_60_mph,
        sum(lost_productivity_35_mph) as lost_productivity_35_mph,
        sum(lost_productivity_40_mph) as lost_productivity_40_mph,
        sum(lost_productivity_45_mph) as lost_productivity_45_mph,
        sum(lost_productivity_50_mph) as lost_productivity_50_mph,
        sum(lost_productivity_55_mph) as lost_productivity_55_mph,
        sum(lost_productivity_60_mph) as lost_productivity_60_mph
    from station_five_mins_data
    group by id, sample_date, sample_hour
),

-- read spatial characteristics
hourly_station_level_spatial_temporal_metrics as (
    select
        hourly_temporal_metrics.id,
        hourly_temporal_metrics.sample_date,
        hourly_temporal_metrics.sample_hour,
        hourly_temporal_metrics.hourly_speed,
        hourly_temporal_metrics.volume_sum,
        hourly_temporal_metrics.occupancy_avg,
        hourly_temporal_metrics.hourly_vmt,
        hourly_temporal_metrics.hourly_vht,
        hourly_temporal_metrics.hourly_q_value,
        hourly_temporal_metrics.hourly_tti,
        hourly_temporal_metrics.delay_35_mph,
        hourly_temporal_metrics.delay_40_mph,
        hourly_temporal_metrics.delay_45_mph,
        hourly_temporal_metrics.delay_50_mph,
        hourly_temporal_metrics.delay_55_mph,
        hourly_temporal_metrics.delay_60_mph,
        hourly_temporal_metrics.lost_productivity_35_mph,
        hourly_temporal_metrics.lost_productivity_40_mph,
        hourly_temporal_metrics.lost_productivity_45_mph,
        hourly_temporal_metrics.lost_productivity_50_mph,
        hourly_temporal_metrics.lost_productivity_55_mph,
        hourly_temporal_metrics.lost_productivity_60_mph,
        station_meta_data.city,
        station_meta_data.county,
        station_meta_data.district,
        station_meta_data.type
    from {{ ref('int_clearinghouse__most_recent_station_meta') }} as station_meta_data
    inner join hourly_temporal_metrics
        on
            station_meta_data.id = hourly_temporal_metrics.id
)

select * from hourly_station_level_spatial_temporal_metrics

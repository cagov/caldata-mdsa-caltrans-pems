{{ config(materialized='table') }}

-- read the volume, occupancy and speed hourly data
with station_hourly_data as (
    select
        id,
        sample_date,
        city,
        county,
        district,
        type,
        volume_sum,
        occupancy_avg,
        hourly_speed,
        hourly_vmt,
        hourly_vht,
        delay_35_mph,
        delay_40_mph,
        delay_45_mph,
        delay_50_mph,
        delay_55_mph,
        delay_60_mph,
        extract(day from sample_date) as sample_day
    from {{ ref('int_clearninghouse__station_temporal_hourly_agg') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_station_level_spatial_temporal_metrics as (
    select
        id,
        sample_date,
        city,
        county,
        district,
        type,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * hourly_speed) / nullifzero(sum(volume_sum)) as daily_speed,
        sum(hourly_vmt) as daily_vmt,
        sum(hourly_vht) as daily_vht,
        daily_vmt / nullifzero(daily_vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti,
        sum(delay_35_mph) as delay_35_mph,
        sum(delay_40_mph) as delay_40_mph,
        sum(delay_45_mph) as delay_45_mph,
        sum(delay_50_mph) as delay_50_mph,
        sum(delay_55_mph) as delay_55_mph,
        sum(delay_60_mph) as delay_60_mph
    from station_hourly_data
    group by id, sample_date, sample_day, city, county, district, type
)

select * from daily_station_level_spatial_temporal_metrics

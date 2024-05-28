{{ config(materialized='table') }}

-- read the volume, occupancy and speed hourly data
with station_hourly_data as (
    select
        id,
        lane,
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
        lost_productivity_35_mp,
        lost_productivity_40_mp,
        lost_productivity_45_mp,
        lost_productivity_50_mp,
        lost_productivity_55_mp,
        lost_productivity_60_mp
    from {{ ref('int_clearinghouse__temporal_hourly_agg') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_spatial_temporal_metrics as (
    select
        id,
        lane,
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
        daily_vmt / nullifzero(daily_vht) as daily_q_value,
        -- travel time
        60 / nullifzero(daily_q_value) as daily_tti,
        sum(delay_35_mph) as delay_35_mph,
        sum(delay_40_mph) as delay_40_mph,
        sum(delay_45_mph) as delay_45_mph,
        sum(delay_50_mph) as delay_50_mph,
        sum(delay_55_mph) as delay_55_mph,
        sum(delay_60_mph) as delay_60_mph,
        sum(lost_productivity_35_m) as lost_productivity_35_m,
        sum(lost_productivity_40_m) as lost_productivity_40_m,
        sum(lost_productivity_45_m) as lost_productivity_45_m,
        sum(lost_productivity_50_m) as lost_productivity_50_m,
        sum(lost_productivity_55_m) as lost_productivity_55_m,
        sum(lost_productivity_60_m) as lost_productivity_60_m
    from station_hourly_data
    group by id, sample_date, lane, city, county, district, type
)

select * from daily_spatial_temporal_metrics

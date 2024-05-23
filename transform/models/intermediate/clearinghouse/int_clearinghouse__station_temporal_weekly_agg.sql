{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        id,
        city,
        county,
        district,
        type,
        volume_sum,
        occupancy_avg,
        daily_speed,
        daily_vmt,
        daily_vht,
        delay_35_mph,
        delay_40_mph,
        delay_45_mph,
        delay_50_mph,
        delay_55_mph,
        delay_60_mph,
        extract(week from sample_date) as sample_week
    from {{ ref('int_clearinghouse__station_temporal_daily_agg') }}
),

-- now aggregate daily volume, occupancy and speed to weekly
weekly_station_level_spatial_temporal_metrics as (
    select
        id,
        city,
        county,
        district,
        type,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * daily_speed) / nullifzero(sum(volume_sum)) as weekly_speed,
        sum(daily_vmt) as weekly_vmt,
        sum(daily_vht) as weekly_vht,
        weekly_vmt / nullifzero(weekly_vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti,
        sum(delay_35_mph) as delay_35_mph,
        sum(delay_40_mph) as delay_40_mph,
        sum(delay_45_mph) as delay_45_mph,
        sum(delay_50_mph) as delay_50_mph,
        sum(delay_55_mph) as delay_55_mph,
        sum(delay_60_mph) as delay_60_mph
    from station_daily_data
    group by id, sample_week, city, county, district, type
)

select * from weekly_station_level_spatial_temporal_metrics

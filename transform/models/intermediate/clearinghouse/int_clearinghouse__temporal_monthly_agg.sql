{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        id,
        lane,
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
        extract(month from sample_date) as sample_month
    from {{ ref('int_clearinghouse__temporal_daily_agg') }}
),

-- now aggregate daily volume, occupancy and speed to weekly
monthly_spatial_temporal_metrics as (
    select
        id,
        lane,
        city,
        county,
        district,
        type,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * daily_speed) / nullifzero(sum(volume_sum)) as monthly_speed,
        sum(daily_vmt) as monthly_vmt,
        sum(daily_vht) as monthly_vht,
        monthly_vmt / nullifzero(monthly_vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti,
        sum(delay_35_mph) as delay_35_mph,
        sum(delay_40_mph) as delay_40_mph,
        sum(delay_45_mph) as delay_45_mph,
        sum(delay_50_mph) as delay_50_mph,
        sum(delay_55_mph) as delay_55_mph,
        sum(delay_60_mph) as delay_60_mph
    from station_daily_data
    group by id, sample_month, lane, city, county, district, type
)

select * from monthly_spatial_temporal_metrics

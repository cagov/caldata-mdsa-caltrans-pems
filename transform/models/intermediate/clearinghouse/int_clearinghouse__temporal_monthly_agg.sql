{{ config(materialized='table') }}

-- read the volume, occupancy and speed five minutes data
with station_daily_data as (
    select
        id,
        sample_date,
        lane,
        sample_day,
        daily_speed,
        daily_volume,
        daily_occupancy,
        extract(month from sample_date) as sample_month
    from {{ ref('int_clearinghouse__temporal_daily_agg') }}
),

-- now aggregate daily volume, occupancy and speed
monthly_temporal_metrics as (
    select
        id,
        sample_date,
        lane,
        sample_month,
        sum(daily_volume) as monthly_volume,
        avg(daily_occupancy) as monthly_occupancy,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as monthly_speed
    from station_daily_data
    group by id, sample_date, sample_month, lane
)

select * from monthly_temporal_metrics

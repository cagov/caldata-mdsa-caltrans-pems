{{ config(materialized='table') }}

-- read the volume, occupancy and speed five minutes data
with station_hourly_data as (
    select
        id,
        sample_date,
        lane,
        sample_hour,
        hourly_speed,
        hourly_volume,
        hourly_occupancy,
        extract(day from sample_date) as sample_day
    from {{ ref('int_clearinghouse__temporal_hourly_agg') }}
),

-- now aggregate hourly volume, occupancy and speed
daily_temporal_metrics as (
    select
        id,
        sample_date,
        lane,
        sample_day,
        sum(hourly_volume) as daily_volume,
        avg(hourly_occupancy) as daily_occupancy,
        sum(hourly_volume * hourly_speed) / nullifzero(sum(hourly_volume)) as daily_speed
    from station_hourly_data
    group by id, sample_date, sample_day, lane
)

select * from daily_temporal_metrics

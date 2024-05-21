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
        extract(week from sample_date) as sample_week
    from {{ ref('int_clearinghouse__temporal_daily_agg') }}
),

-- now aggregate daily volume, occupancy and speed
weekly_temporal_metrics as (
    select
        id,
        sample_date,
        lane,
        sample_week,
        sum(daily_volume) as weekly_volume,
        avg(daily_occupancy) as weekly_occupancy,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as weekly_speed
    from station_daily_data
    group by id, sample_date, sample_week, lane
)

select * from weekly_temporal_metrics

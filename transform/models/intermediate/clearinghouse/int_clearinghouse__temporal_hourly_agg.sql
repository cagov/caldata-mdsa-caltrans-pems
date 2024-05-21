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
        speed_weighted,
        extract(hour from sample_timestamp) as sample_hour,
        coalesce(speed_weighted, (volume_sum * 22) / nullifzero(occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from {{ ref('int_clearinghouse__five_minute_station_agg') }}
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
    from station_five_mins_data
    group by id, sample_date, sample_hour, lane
)

select * from hourly_temporal_metrics

with

source as (
    select * from {{ ref("stg_pems__station_30SecRaw") }}
),

samples_per_station as (
    select
        station_id,
        SUM(lane_1_count) as lane1_sample_count,
        SUM(lane_2_count) as lane2_sample_count,
        SUM(lane_3_count) as lane3_sample_count,
        SUM(lane_4_count) as lane4_sample_count,
        SUM(lane_5_count) as lane5_sample_count,
        SUM(lane_6_count) as lane6_sample_count,
        SUM(lane_7_count) as lane7_sample_count,
        SUM(lane_8_count) as lane8_sample_count
    from source
    where
        sample_timestamp >= '2023-01-01T05:00:00'
        and sample_timestamp <= '2023-01-01T21:59:59'

    group by station_id
)

select * from samples_per_station
order by station_id desc

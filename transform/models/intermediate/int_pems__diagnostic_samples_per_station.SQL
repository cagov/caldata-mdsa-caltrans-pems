with

source as (
    select * from {{ ref("stg_pems__station_30SecRaw") }}
),

samples_per_station as (
    select
        station_id,
        --Sum of total 30 second raw data samples by station and lane
        SUM(lane_1_count) as lane1_sample_count,
        SUM(lane_2_count) as lane2_sample_count,
        SUM(lane_3_count) as lane3_sample_count,
        SUM(lane_4_count) as lane4_sample_count,
        SUM(lane_5_count) as lane5_sample_count,
        SUM(lane_6_count) as lane6_sample_count,
        SUM(lane_7_count) as lane7_sample_count,
        SUM(lane_8_count) as lane8_sample_count,
        --Sum of Zero flow values in 30 second raw data by station and lane
        SUM(lane_1_zeroflowcount) as lane1_zeroflow_count,
        SUM(lane_2_zeroflowcount) as lane2_zeroflow_count,
        SUM(lane_3_zeroflowcount) as lane3_zeroflow_count,
        SUM(lane_4_zeroflowcount) as lane4_zeroflow_count,
        SUM(lane_5_zeroflowcount) as lane5_zeroflow_count,
        SUM(lane_6_zeroflowcount) as lane6_zeroflow_count,
        SUM(lane_7_zeroflowcount) as lane7_zeroflow_count,
        SUM(lane_8_zeroflowcount) as lane8_zeroflow_count,
        --Sum of zero occupancy in 30 second raw data by station and lane
        SUM(lane_1_zeroocccount) as lane1_zeroocc_count,
        SUM(lane_2_zeroocccount) as lane2_zeroocc_count,
        SUM(lane_3_zeroocccount) as lane3_zeroocc_count,
        SUM(lane_4_zeroocccount) as lane4_zeroocc_count,
        SUM(lane_5_zeroocccount) as lane5_zeroocc_count,
        SUM(lane_6_zeroocccount) as lane6_zeroocc_count,
        SUM(lane_7_zeroocccount) as lane7_zeroocc_count,
        SUM(lane_8_zeroocccount) as lane8_zeroocc_count
    from source
    where
        sample_timestamp >= '2023-01-01T05:00:00'
        and sample_timestamp <= '2023-01-01T21:59:59'
    group by station_id
)

select * from samples_per_station
order by station_id desc

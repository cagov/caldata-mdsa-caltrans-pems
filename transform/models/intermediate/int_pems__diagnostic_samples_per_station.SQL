with

source as (
    select * from {{ ref("stg_pems__station_30SecRaw") }}
),

samples_per_station as (
    select
        station_id,
        --Sum of total 30 second raw data samples by station and lane
        SUM(lane_1_cnt) as lane1_sample_cnt,
        SUM(lane_2_cnt) as lane2_sample_cnt,
        SUM(lane_3_cnt) as lane3_sample_cnt,
        SUM(lane_4_cnt) as lane4_sample_cnt,
        SUM(lane_5_cnt) as lane5_sample_cnt,
        SUM(lane_6_cnt) as lane6_sample_cnt,
        SUM(lane_7_cnt) as lane7_sample_cnt,
        SUM(lane_8_cnt) as lane8_sample_cnt,
        --Sum of Zero flow values in 30 second raw data by station and lane
        SUM(lane_1_zeroflowcnt) as lane1_zeroflow_cnt,
        SUM(lane_2_zeroflowcnt) as lane2_zeroflow_cnt,
        SUM(lane_3_zeroflowcnt) as lane3_zeroflow_cnt,
        SUM(lane_4_zeroflowcnt) as lane4_zeroflow_cnt,
        SUM(lane_5_zeroflowcnt) as lane5_zeroflow_cnt,
        SUM(lane_6_zeroflowcnt) as lane6_zeroflow_cnt,
        SUM(lane_7_zeroflowcnt) as lane7_zeroflow_cnt,
        SUM(lane_8_zeroflowcnt) as lane8_zeroflow_cnt,
        --Sum of zero occupancy in 30 second raw data by station and lane
        SUM(lane_1_zeroocccnt) as lane1_zeroocc_cnt,
        SUM(lane_2_zeroocccnt) as lane2_zeroocc_cnt,
        SUM(lane_3_zeroocccnt) as lane3_zeroocc_cnt,
        SUM(lane_4_zeroocccnt) as lane4_zeroocc_cnt,
        SUM(lane_5_zeroocccnt) as lane5_zeroocc_cnt,
        SUM(lane_6_zeroocccnt) as lane6_zeroocc_cnt,
        SUM(lane_7_zeroocccnt) as lane7_zeroocc_cnt,
        SUM(lane_8_zeroocccnt) as lane8_zeroocc_cnt,
        --Sum of zero flow and not null occupancy in 30 second raw data by station and lane
        SUM(lane_1_zf_nno_cnt) as lane1_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane2_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane3_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane4_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane5_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane6_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane7_zf_nno_cnt,
        SUM(lane_1_zf_nno_cnt) as lane8_zf_nno_cnt,
        --Sum of zero occupancy and not null flow in 30 second raw data by station and lane
        SUM(lane_1_nnf_zo_cnt) as lane1_nnf_zo_cnt,
        SUM(lane_2_nnf_zo_cnt) as lane2_nnf_zo_cnt,
        SUM(lane_3_nnf_zo_cnt) as lane3_nnf_zo_cnt,
        SUM(lane_4_nnf_zo_cnt) as lane4_nnf_zo_cnt,
        SUM(lane_5_nnf_zo_cnt) as lane5_nnf_zo_cnt,
        SUM(lane_6_nnf_zo_cnt) as lane6_nnf_zo_cnt,
        SUM(lane_7_nnf_zo_cnt) as lane7_nnf_zo_cnt,
        SUM(lane_8_nnf_zo_cnt) as lane8_nnf_zo_cnt
    from source
    where
        TO_TIME(sample_timestamp) >= '05:00:00'
        and TO_TIME(sample_timestamp) <= '21:59:59'
    group by station_id
)

select * from samples_per_station
order by station_id desc

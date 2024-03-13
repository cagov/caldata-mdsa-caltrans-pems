with

source as (
    select * from {{ source("clearinghouse", "station_raw") }}
    /*
    Currently looks at the same day in previous year. This should be updated to
    look at the previous day once the data refresh brings in more current data
    */
    where
        DATE(sample_date) = DATEADD(year, -1, CURRENT_DATE())
        and TO_TIME(sample_timestamp) >= '05:00:00'
        and TO_TIME(sample_timestamp) <= '21:59:59'
),

samples_per_station as (
    select
        sample_date,
        id as station_id,
        /*
        This following counts a sample if the flow and occupancy values contain any value
        based on 30 second raw data recieved per station, lane and time. Null values
        in flow and occupancy are currently counted as 0 but if these need to be treated
        differently the code should be updated as needed to accomodate such a scenario.
        */
        COUNT_IF(flow_1 is not null and occupancy_1 is not null) as lane1_sample_cnt,
        COUNT_IF(flow_2 is not null and occupancy_2 is not null) as lane2_sample_cnt,
        COUNT_IF(flow_3 is not null and occupancy_3 is not null) as lane3_sample_cnt,
        COUNT_IF(flow_4 is not null and occupancy_4 is not null) as lane4_sample_cnt,
        COUNT_IF(flow_5 is not null and occupancy_5 is not null) as lane5_sample_cnt,
        COUNT_IF(flow_6 is not null and occupancy_6 is not null) as lane6_sample_cnt,
        COUNT_IF(flow_7 is not null and occupancy_7 is not null) as lane7_sample_cnt,
        COUNT_IF(flow_8 is not null and occupancy_8 is not null) as lane8_sample_cnt,
        /*
        The following code will count how many times a 30 second raw flow value equals 0
        for a given station and associated lane
        */
        COUNT_IF(flow_1 = 0) as lane1_zeroflow_cnt,
        COUNT_IF(flow_2 = 0) as lane2_zeroflow_cnt,
        COUNT_IF(flow_3 = 0) as lane3_zeroflow_cnt,
        COUNT_IF(flow_4 = 0) as lane4_zeroflow_cnt,
        COUNT_IF(flow_5 = 0) as lane5_zeroflow_cnt,
        COUNT_IF(flow_6 = 0) as lane6_zeroflow_cnt,
        COUNT_IF(flow_7 = 0) as lane7_zeroflow_cnt,
        COUNT_IF(flow_8 = 0) as lane8_zeroflow_cnt,
        /*
        The following code will count how many times a 30 second raw occupancy value equals 0
        for a given station and associated lane
        */
        COUNT_IF(occupancy_1 = 0) as lane1_zeroocc_cnt,
        COUNT_IF(occupancy_2 = 0) as lane2_zeroocc_cnt,
        COUNT_IF(occupancy_3 = 0) as lane3_zeroocc_cnt,
        COUNT_IF(occupancy_4 = 0) as lane4_zeroocc_cnt,
        COUNT_IF(occupancy_5 = 0) as lane5_zeroocc_cnt,
        COUNT_IF(occupancy_6 = 0) as lane6_zeroocc_cnt,
        COUNT_IF(occupancy_7 = 0) as lane7_zeroocc_cnt,
        COUNT_IF(occupancy_8 = 0) as lane8_zeroocc_cnt,
        /*
        This code counts a sample if the flow is 0 and occupancy value > 0
        based on 30 second raw data recieved per station, lane and time.
        */
        COUNT_IF(flow_1 = 0 and occupancy_1 > 0) as lane1_zf_nno_cnt,
        COUNT_IF(flow_2 = 0 and occupancy_2 > 0) as lane2_zf_nno_cnt,
        COUNT_IF(flow_3 = 0 and occupancy_3 > 0) as lane3_zf_nno_cnt,
        COUNT_IF(flow_4 = 0 and occupancy_4 > 0) as lane4_zf_nno_cnt,
        COUNT_IF(flow_5 = 0 and occupancy_5 > 0) as lane5_zf_nno_cnt,
        COUNT_IF(flow_6 = 0 and occupancy_6 > 0) as lane6_zf_nno_cnt,
        COUNT_IF(flow_7 = 0 and occupancy_7 > 0) as lane7_zf_nno_cnt,
        COUNT_IF(flow_8 = 0 and occupancy_8 > 0) as lane8_zf_nno_cnt,
        /*
        This code counts a sample if the occupancy is 0 and a flow value > 0
        based on 30 second raw data recieved per station, lane and time.
        */
        COUNT_IF(flow_1 > 0 and occupancy_1 = 0) as lane1_nnf_zo_cnt,
        COUNT_IF(flow_2 > 0 and occupancy_2 = 0) as lane2_nnf_zo_cnt,
        COUNT_IF(flow_3 > 0 and occupancy_3 = 0) as lane3_nnf_zo_cnt,
        COUNT_IF(flow_4 > 0 and occupancy_4 = 0) as lane4_nnf_zo_cnt,
        COUNT_IF(flow_5 > 0 and occupancy_5 = 0) as lane5_nnf_zo_cnt,
        COUNT_IF(flow_6 > 0 and occupancy_6 = 0) as lane6_nnf_zo_cnt,
        COUNT_IF(flow_7 > 0 and occupancy_7 = 0) as lane7_nnf_zo_cnt,
        COUNT_IF(flow_8 > 0 and occupancy_8 = 0) as lane8_nnf_zo_cnt
    from source
    group by station_id, sample_date
)

select * from samples_per_station
order by station_id desc

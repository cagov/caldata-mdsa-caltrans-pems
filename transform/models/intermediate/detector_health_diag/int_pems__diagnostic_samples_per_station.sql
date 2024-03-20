{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["station_id", "sample_date"],
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

with
source as (
    select * from {{ ref ('stg_clearinghouse__station_raw') }}
    where TO_TIME(sample_timestamp) >= '05:00:00' and TO_TIME(sample_timestamp) <= '21:59:59'
    {% if is_incremental() %}
        where sample_date > 
            (select dateadd(day, -2, max(sample_date))
            from {{ this }})            
    {% endif %}
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
    group by id, sample_date
),

det_diag_too_few_samples as (
    select
        *,
        -- # of samples < 60% of the max collected samples during the test period
        -- max value: 2 samples per minute times 60 mins/hr times 17 hours in a day which == 1224
        -- btwn 1 and 1224 is too few samples
        COALESCE(lane1_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane1_too_few_samples,
        COALESCE(lane2_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane2_too_few_samples,
        COALESCE(lane3_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane3_too_few_samples,
        COALESCE(lane4_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane4_too_few_samples,
        COALESCE(lane5_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane5_too_few_samples,
        COALESCE(lane6_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane6_too_few_samples,
        COALESCE(lane7_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane7_too_few_samples,
        COALESCE(lane8_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane8_too_few_samples

    from samples_per_station
),

/*
This SQL file counts the number of flow and occupancy values that exceed
detector threshold values for a station based on the station set assignment.
*/
high_flow_samples_per_station as (
    select
        source.sample_date,
        source.id,
        COUNT_IF(source.flow_1 > ddsa.dt_value) as lane1_highflow_cnt,
        COUNT_IF(source.flow_2 > ddsa.dt_value) as lane2_highflow_cnt,
        COUNT_IF(source.flow_3 > ddsa.dt_value) as lane3_highflow_cnt,
        COUNT_IF(source.flow_4 > ddsa.dt_value) as lane4_highflow_cnt,
        COUNT_IF(source.flow_5 > ddsa.dt_value) as lane5_highflow_cnt,
        COUNT_IF(source.flow_6 > ddsa.dt_value) as lane6_highflow_cnt,
        COUNT_IF(source.flow_7 > ddsa.dt_value) as lane7_highflow_cnt,
        COUNT_IF(source.flow_8 > ddsa.dt_value) as lane8_highflow_cnt

    from source
    inner join {{ ref('int_pems__det_diag_set_assignment') }} as ddsa
        on
            source.id = ddsa.station_id
            and ddsa.dt_name = 'high_flow'
    group by source.id, source.sample_date

),

high_occupancy_samples_per_station as (
    select
        source.sample_date,
        source.id,
        COUNT_IF(source.occupancy_1 > ddsa.dt_value) as lane1_highocc_cnt,
        COUNT_IF(source.occupancy_2 > ddsa.dt_value) as lane2_highocc_cnt,
        COUNT_IF(source.occupancy_3 > ddsa.dt_value) as lane3_highocc_cnt,
        COUNT_IF(source.occupancy_4 > ddsa.dt_value) as lane4_highocc_cnt,
        COUNT_IF(source.occupancy_5 > ddsa.dt_value) as lane5_highocc_cnt,
        COUNT_IF(source.occupancy_6 > ddsa.dt_value) as lane6_highocc_cnt,
        COUNT_IF(source.occupancy_7 > ddsa.dt_value) as lane7_highocc_cnt,
        COUNT_IF(source.occupancy_8 > ddsa.dt_value) as lane8_highocc_cnt

    from source
    inner join {{ ref('int_pems__det_diag_set_assignment') }} as ddsa
        on
            source.id = ddsa.station_id
            and ddsa.dt_name = 'high_occ'
    group by source.id, source.sample_date
)

select
    dd.*,
    hf.* exclude (id, sample_date),
    ho.* exclude (id, sample_date)
from det_diag_too_few_samples as dd, high_flow_samples_per_station as hf, high_occupancy_samples_per_station as ho
order by dd.station_id desc

with
source as (
    select * from {{ ref("stg_clearinghouse__station_raw") }}

    /*
    Currently looks at the same day in previous year. This should be updated to
    look at the previous day once the data refresh brings in more current data
    */
    where
        sample_date = DATEADD(year, -1, CURRENT_DATE())
        and TO_TIME(sample_timestamp) >= '05:00:00'
        and TO_TIME(sample_timestamp) <= '21:59:59'
),

/*
This SQL file counts the number of flow and occupancy values that exceed
detector threshold values for a station based on the station set assignment.
*/
high_flow_samples_per_station as (
    select
        source.sample_date,
        source.id as station_id,
        COUNT_IF(source.flow_1 > ddtv.dt_value) as lane1_highflow_cnt,
        COUNT_IF(source.flow_2 > ddtv.dt_value) as lane2_highflow_cnt,
        COUNT_IF(source.flow_3 > ddtv.dt_value) as lane3_highflow_cnt,
        COUNT_IF(source.flow_4 > ddtv.dt_value) as lane4_highflow_cnt,
        COUNT_IF(source.flow_5 > ddtv.dt_value) as lane5_highflow_cnt,
        COUNT_IF(source.flow_6 > ddtv.dt_value) as lane6_highflow_cnt,
        COUNT_IF(source.flow_7 > ddtv.dt_value) as lane7_highflow_cnt,
        COUNT_IF(source.flow_8 > ddtv.dt_value) as lane8_highflow_cnt

    from source
    inner join {{ ref("stg_pems__det_diag_threshold_station") }} as ddtv
        on
            source.id = ddtv.station_id
            and ddtv.dt_name = 'high_flow'

    group by source.sample_date, source.id
),

high_occupancy_samples_per_station as (
    select
        source.sample_date,
        source.id as station_id,
        COUNT_IF(source.occupancy_1 > ddtv.dt_value) as lane1_highocc_cnt,
        COUNT_IF(source.occupancy_2 > ddtv.dt_value) as lane2_highocc_cnt,
        COUNT_IF(source.occupancy_3 > ddtv.dt_value) as lane3_highocc_cnt,
        COUNT_IF(source.occupancy_4 > ddtv.dt_value) as lane4_highocc_cnt,
        COUNT_IF(source.occupancy_5 > ddtv.dt_value) as lane5_highocc_cnt,
        COUNT_IF(source.occupancy_6 > ddtv.dt_value) as lane6_highocc_cnt,
        COUNT_IF(source.occupancy_7 > ddtv.dt_value) as lane7_highocc_cnt,
        COUNT_IF(source.occupancy_8 > ddtv.dt_value) as lane8_highocc_cnt

    from source
    inner join {{ ref("stg_pems__det_diag_threshold_station") }} as ddtv
        on
            source.id = ddtv.station_id
            and ddtv.dt_name = 'high_occ'

    group by source.sample_date, source.id

)

select
    hfsps.sample_date,
    hfsps.station_id,
    hfsps.lane1_highflow_cnt,
    hfsps.lane2_highflow_cnt,
    hfsps.lane3_highflow_cnt,
    hfsps.lane4_highflow_cnt,
    hfsps.lane5_highflow_cnt,
    hfsps.lane6_highflow_cnt,
    hfsps.lane7_highflow_cnt,
    hfsps.lane8_highflow_cnt,
    hosps.lane1_highocc_cnt,
    hosps.lane2_highocc_cnt,
    hosps.lane3_highocc_cnt,
    hosps.lane4_highocc_cnt,
    hosps.lane5_highocc_cnt,
    hosps.lane6_highocc_cnt,
    hosps.lane7_highocc_cnt,
    hosps.lane8_highocc_cnt

from high_flow_samples_per_station as hfsps
inner join high_occupancy_samples_per_station as hosps
    on
        hfsps.station_id = hosps.station_id
        and hfsps.sample_date = hosps.sample_date

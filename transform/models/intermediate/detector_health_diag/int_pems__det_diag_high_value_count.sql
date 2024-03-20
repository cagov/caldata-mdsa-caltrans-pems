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

det_diag_set_assign as (
    /*
    This SQL file assigns which sets of calculations will be used for
    a station based on information in from the station metadata
    The DET_DIAG_SET_ID varialbe assigns 1 of 5 values for detector
    diagnostic evaluations. The DET_DIAG_METHOD_ID variable assigns
    1 of 2 values for detector diagnostic evaluations.
    */
    select
        meta_date,
        id as station_id,
        district,
        type,
        case
            /*when LIKE(UPPER(THRESHOLD_SET), "LOW%") then "Low_Volume"
            This value is currently in district config file but not in
            our current metadata files
            when LIKE(UPPER(THRESHOLD_SET), "RURAL%") then "Rural"
            We need the definition of when a station is considered
            Rural from Iteris
            */
            when district = 11 then 'Urban_D11'
            when district = 6 then 'D6_Ramps'
            else 'Urban'
        end as det_diag_set_id,
        case
            when type = 'OR' then 'ramp'
            else 'mainline'
        end as det_diag_method_id

    from {{ ref('int_vds__most_recent_station_meta') }}
),

det_diag_threshold_values as (
    /*
    This SQL file assigns which detector threshold values will be used
    for a station based on information from the station metadata.
    */
    select
        ddsa.*,
        dtv.dt_name,
        dtv.dt_value

    from det_diag_set_assign as ddsa
    inner join {{ ref('diagnostic_threshold_values') }} as dtv
        on
            ddsa.det_diag_set_id = dtv.dt_set_id
            and ddsa.det_diag_method_id = dtv.dt_method
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
    inner join det_diag_threshold_values as ddtv
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
    inner join det_diag_threshold_values as ddtv
        on
            source.id = ddtv.station_id
            and ddtv.dt_name = 'high_occ'

    group by source.sample_date, source.id

)

select 
    hfsps.*,
    hosps.* exclude(station_id, sample_date)

from high_flow_samples_per_station as hfsps
left join high_occupancy_samples_per_station as hosps
    on
        hfsps.station_id = hosps.station_id
        and hfsps.sample_date = hosps.sample_date
order by station_id desc

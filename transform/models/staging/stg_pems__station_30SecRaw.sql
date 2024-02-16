select
    ID as STATION_ID,
    SAMPLE_TIMESTAMP,
    case
        when FLOW_1 is not NULL and OCCUPANCY_1 is not NULL then 1
        else 0
    end as LANE_1_COUNT,
    case
        when FLOW_2 is not NULL and OCCUPANCY_2 is not NULL then 1
        else 0
    end as LANE_2_COUNT,
    case
        when FLOW_3 is not NULL and OCCUPANCY_3 is not NULL then 1
        else 0
    end as LANE_3_COUNT,
    case
        when FLOW_4 is not NULL and OCCUPANCY_4 is not NULL then 1
        else 0
    end as LANE_4_COUNT,
    case
        when FLOW_5 is not NULL and OCCUPANCY_5 is not NULL then 1
        else 0
    end as LANE_5_COUNT,
    case
        when FLOW_6 is not NULL and OCCUPANCY_6 is not NULL then 1
        else 0
    end as LANE_6_COUNT,
    case
        when FLOW_7 is not NULL and OCCUPANCY_7 is not NULL then 1
        else 0
    end as LANE_7_COUNT,
    case
        when FLOW_8 is not NULL and OCCUPANCY_8 is not NULL then 1
        else 0
    end as LANE_8_COUNT

from {{ source("CLEARINGHOUSE", "STATION_RAW") }}

where DATE(SAMPLE_TIMESTAMP) = '2023-01-01'

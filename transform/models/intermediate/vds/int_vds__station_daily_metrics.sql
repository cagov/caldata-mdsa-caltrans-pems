{{ config(materialized='table') }}

select
    ID,
    SAMPLE_DATE,
    count(FLOW_1) as N_SAMPLES_LANE_1,
    count(FLOW_2) as N_SAMPLES_LANE_2,
    count(FLOW_3) as N_SAMPLES_LANE_3,
    count(FLOW_4) as N_SAMPLES_LANE_4,
    count(FLOW_5) as N_SAMPLES_LANE_5,
    count(FLOW_6) as N_SAMPLES_LANE_6,
    count(FLOW_7) as N_SAMPLES_LANE_7,
    count(FLOW_8) as N_SAMPLES_LANE_8,
    count_if(FLOW_1 = 0) as ZERO_FLOW_SAMPLES_LANE_1,
    count_if(FLOW_2 = 0) as ZERO_FLOW_SAMPLES_LANE_2,
    count_if(FLOW_3 = 0) as ZERO_FLOW_SAMPLES_LANE_3,
    count_if(FLOW_4 = 0) as ZERO_FLOW_SAMPLES_LANE_4,
    count_if(FLOW_5 = 0) as ZERO_FLOW_SAMPLES_LANE_5,
    count_if(FLOW_6 = 0) as ZERO_FLOW_SAMPLES_LANE_6,
    count_if(FLOW_7 = 0) as ZERO_FLOW_SAMPLES_LANE_7,
    count_if(FLOW_8 = 0) as ZERO_FLOW_SAMPLES_LANE_8,
    count_if(OCCUPANCY_1 = 0) as ZERO_OCC_SAMPLES_LANE_1,
    count_if(OCCUPANCY_2 = 0) as ZERO_OCC_SAMPLES_LANE_2,
    count_if(OCCUPANCY_3 = 0) as ZERO_OCC_SAMPLES_LANE_3,
    count_if(OCCUPANCY_4 = 0) as ZERO_OCC_SAMPLES_LANE_4,
    count_if(OCCUPANCY_5 = 0) as ZERO_OCC_SAMPLES_LANE_5,
    count_if(OCCUPANCY_6 = 0) as ZERO_OCC_SAMPLES_LANE_6,
    count_if(OCCUPANCY_7 = 0) as ZERO_OCC_SAMPLES_LANE_7,
    count_if(OCCUPANCY_8 = 0) as ZERO_OCC_SAMPLES_LANE_8
from {{ ref('stg_clearinghouse__station_raw') }}
group by ID, SAMPLE_DATE
having SAMPLE_DATE > '2023-11-01'

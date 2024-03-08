{{ config(
        materialized='incremental',
        unique_key=['ID', 'SAMPLE_DATE']
    )
}}

with station_raw as (
    select * from {{ ref('stg_clearinghouse__station_raw') }}
    {% if is_incremental() %}
        where sample_date > (
            select dateadd(day, -2, max(sample_date)) from {{ this }}
        )
    {% endif %}
)

select
    id,
    sample_date,
    count(flow_1) as n_samples_lane_1,
    count(flow_2) as n_samples_lane_2,
    count(flow_3) as n_samples_lane_3,
    count(flow_4) as n_samples_lane_4,
    count(flow_5) as n_samples_lane_5,
    count(flow_6) as n_samples_lane_6,
    count(flow_7) as n_samples_lane_7,
    count(flow_8) as n_samples_lane_8,
    count_if(flow_1 = 0) as zero_flow_samples_lane_1,
    count_if(flow_2 = 0) as zero_flow_samples_lane_2,
    count_if(flow_3 = 0) as zero_flow_samples_lane_3,
    count_if(flow_4 = 0) as zero_flow_samples_lane_4,
    count_if(flow_5 = 0) as zero_flow_samples_lane_5,
    count_if(flow_6 = 0) as zero_flow_samples_lane_6,
    count_if(flow_7 = 0) as zero_flow_samples_lane_7,
    count_if(flow_8 = 0) as zero_flow_samples_lane_8,
    count_if(occupancy_1 = 0) as zero_occ_samples_lane_1,
    count_if(occupancy_2 = 0) as zero_occ_samples_lane_2,
    count_if(occupancy_3 = 0) as zero_occ_samples_lane_3,
    count_if(occupancy_4 = 0) as zero_occ_samples_lane_4,
    count_if(occupancy_5 = 0) as zero_occ_samples_lane_5,
    count_if(occupancy_6 = 0) as zero_occ_samples_lane_6,
    count_if(occupancy_7 = 0) as zero_occ_samples_lane_7,
    count_if(occupancy_8 = 0) as zero_occ_samples_lane_8
from station_raw
group by id, sample_date
having sample_date > '2023-11-01'

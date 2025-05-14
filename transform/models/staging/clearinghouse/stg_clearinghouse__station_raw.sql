{% set district_re='clhouse/meta/d(\\\\d{2})' %}

select
    substr(filename, 14, 2)::int as district,
    sample_timestamp,
    sample_date,
    id,
    flow_1 as volume_1,
    occupancy_1,
    speed_1,
    flow_2 as volume_2,
    occupancy_2,
    speed_2,
    flow_3 as volume_3,
    occupancy_3,
    speed_3,
    flow_4 as volume_4,
    occupancy_4,
    speed_4,
    flow_5 as volume_5,
    occupancy_5,
    speed_5,
    flow_6 as volume_6,
    occupancy_6,
    speed_6,
    flow_7 as volume_7,
    occupancy_7,
    speed_7,
    flow_8 as volume_8,
    occupancy_8,
    speed_8
from {{ source('clearinghouse', 'station_raw') }}
where sample_date >= '{{ config.get("begin") }}' -- TODO: possibly unnecessary?

{% set district_re='clhouse/meta/d(\\\\d{2})' %}

select
    substr(s.filename, 14, 2)::int as district,
    s.id,
    s.sample_date,
    s.sample_timestamp,
    lane.value as lane,
    -- neat trick to flatten multiple columns at once rather than using UNPIVOT:
    -- https://stackoverflow.com/questions/36798558/lateral-flatten-two-columns-without-repetition-in-snowflake
    [
        s.flow_1,
        s.flow_2,
        s.flow_3,
        s.flow_4,
        s.flow_5,
        s.flow_6,
        s.flow_7,
        s.flow_8
    ][
        lane.index
    ] as volume,
    [
        s.occupancy_1,
        s.occupancy_2,
        s.occupancy_3,
        s.occupancy_4,
        s.occupancy_5,
        s.occupancy_6,
        s.occupancy_7,
        s.occupancy_8
    ][
        lane.index
    ] as occupancy,
    [
        s.speed_1,
        s.speed_2,
        s.speed_3,
        s.speed_4,
        s.speed_5,
        s.speed_6,
        s.speed_7,
        s.speed_8
    ][
        lane.index
    ] as speed
from {{ source('clearinghouse', 'station_raw') }} as s,
    lateral flatten([1, 2, 3, 4, 5, 6, 7, 8]) as lane
    where s.sample_date >= {{ var("pems_clearinghouse_start_date")}} 
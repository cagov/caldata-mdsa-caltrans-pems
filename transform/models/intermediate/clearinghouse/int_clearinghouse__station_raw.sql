{{ config(materialized="table") }}

with station_raw as (
    select * from {{ ref('stg_clearinghouse__station_raw') }}
),

station_raw_unpivot as (
    select
        s.id as station_id,
        s.district,
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
        ] as flow,
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
        ] as occupancy
    from station_raw as s,
        lateral flatten([1, 2, 3, 4, 5, 6, 7, 8]) as lane
    where s.sample_date = '2024-01-01'
)

select sum(flow) as flow from station_raw_unpivot

{{ config(materialized="table") }}

with station_raw as (
    select * from {{ ref('stg_clearinghouse__station_raw') }}
),

station_status as (
    select
        station_id,
        detector_id,
        -- could do log10 or string parsing here, but perhaps less clear
        case lane_number
            when 1
                then 1
            when 10
                then 2
            when 100
                then 3
            when 1000
                then 4
            when 10000
                then 5
            when 100000
                then 6
            when 1000000
                then 7
            when 10000000
                then 8
        end as lane_number,
        _valid_from,
        _valid_to
    from {{ ref('int_clearinghouse__station_status') }}
),

station_raw_unpivot as (
    select
        s.id as station_id,
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
),

station_raw_with_detector as (
    select
        station_status.detector_id,
        station_raw_unpivot.*
    from station_raw_unpivot
    left join station_status
        on
            station_raw_unpivot.station_id = station_status.station_id
            and station_raw_unpivot.lane = station_status.lane_number
            and station_raw_unpivot.sample_date >= station_status._valid_from
            and station_raw_unpivot.sample_date < coalesce(station_status._valid_to, dateadd('day', 1, current_date()))
)

select * from station_raw_with_detector

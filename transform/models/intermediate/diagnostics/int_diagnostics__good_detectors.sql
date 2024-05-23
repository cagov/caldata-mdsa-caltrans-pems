{{ config(materialized="table") }}

with detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
    where status = 'Good'
),

detectors as (
    select * from {{ ref("int_clearinghouse__station_status") }}
),

detector_status_with_real_lanes as (
    select detector_status.* from detector_status
    inner join detectors
        on
            detector_status.district = detectors.district
            and detector_status.station_id = detectors.station_id
            and detector_status.lane = len(detectors.lane_number::string)
            and detector_status.sample_date >= detectors._valid_from
            and detector_status.sample_date < detectors._valid_to
)

select * from detector_status_with_real_lanes

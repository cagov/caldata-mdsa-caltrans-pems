{{ config(materialized="table") }}

with detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
),

detectors as (
    select * from {{ ref("int_clearinghouse__station_status") }}
    -- Note: some metadata issues can result in duplicate station/lane pairs for
    -- detectors. These are not easy to debug, and we have no reliable way to
    -- associate detectors with actual counts. So we arbitrarily select one
    -- in that case.
    -- TODO: create some sort of diagnostics model that allows us to flag
    -- duplicates so they can be raised to the district station operators.
    qualify row_number() over (partition by station_id, lane_number, meta_date order by detector_id) = 1
),

/* TODO: we should ensure that all of the detectors in the detector
status table are real by filtering out invalid lanes there, rather
than in this model. Once that is done, this model can be deleted */
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

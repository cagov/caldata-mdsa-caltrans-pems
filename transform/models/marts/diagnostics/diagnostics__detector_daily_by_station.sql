{{ config(materialized="table") }}

with

detector_status as (
    select
        *,
        count_if(status = 'Good') as good_detector,
        count_if(status != 'Good') as bad_detector
    from {{ ref("int_diagnostics__detector_status") }}
),

detector_status_by_station as (
    select
        district,
        station_id,
        sample_date,
        count(detector_id) as detector_count,
        avg(sample_ct) as average_sample_count,
        sum(good_detector) as good_detector_count,
        sum(bad_detector) as bad_detector_count
    from detector_status
    group by district, station_id, sample_date
),

dmeta as (
    select * from {{ ref('int_vds__detector_config') }}
),

detector_status_by_station_with_metadata as (
    select
        dsbs.*,
        dmeta.state_postmile,
        dmeta.absolute_postmile,
        dmeta.latitude,
        dmeta.longitude,
        dmeta.physical_lanes,
        dmeta.county,
        dmeta.city,
        dmeta.freeway,
        dmeta.direction,
        dmeta.length
    from detector_status_by_station as dsbs
    inner join dmeta
        on
            dsbs.station_id = dmeta.station_id
            and dsbs.sample_date >= dmeta._valid_from
            and (
                dsbs.sample_date < dmeta._valid_to
                or dmeta._valid_to is null
            )
    where dsbs.sample_date is not null
)

select * from detector_status_by_station_with_metadata

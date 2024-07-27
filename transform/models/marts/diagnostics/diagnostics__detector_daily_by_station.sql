{{ config(materialized="table") }}

with

detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
),

detector_status_with_count as (
    select
        district,
        station_id,
        lane,
        station_type,
        sample_date,
        sample_ct,
        count_if(status = 'Good') as good_detector,
        count_if(status != 'Good') as bad_detector
    from detector_status
    group by district, station_id, lane, station_type, sample_date, sample_ct
),

detector_status_by_station as (
    select
        district,
        station_id,
        station_type,
        sample_date,
        count(lane) as detector_count,
        round(avg(sample_ct)) as average_sample_count,
        sum(good_detector) as good_detector_count,
        sum(bad_detector) as bad_detector_count
    from detector_status_with_count
    group by district, station_id, station_type, sample_date
),

dmeta as (
    select * from {{ ref('int_vds__station_config') }}
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

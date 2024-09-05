{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}

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
        count(*) as detector_count,
        round(avg(sample_ct)) as average_sample_count,
        sum(good_detector) as good_detector_count,
        sum(bad_detector) as bad_detector_count
    from detector_status_with_count
    group by district, station_id, station_type, sample_date
),

detector_status_by_station_with_metadata as (
    select
        dsbs.*,
        detector_status.state_postmile,
        detector_status.absolute_postmile,
        detector_status.latitude,
        detector_status.longitude,
        detector_status.physical_lanes,
        detector_status.county,
        detector_status.city,
        detector_status.freeway,
        detector_status.direction,
        detector_status.length
    from detector_status_by_station as dsbs
    inner join detector_status
        on
            dsbs.station_id = detector_status.station_id
            and dsbs.sample_date = detector_status.active_date
    where dsbs.sample_date is not null
)

select * from detector_status_by_station_with_metadata

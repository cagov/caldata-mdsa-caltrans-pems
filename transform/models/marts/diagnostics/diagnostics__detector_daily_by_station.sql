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
        count_if(status != 'Good') as bad_detector,
        count_if(status = 'Down/No Data') as down_or_no_data,
        count_if(status = 'Insufficient Data') as insufficient_data,
        count_if(status = 'Card Off') as card_off,
        count_if(status = 'High Val') as high_val,
        count_if(status = 'Intermittent') as intermittent,
        count_if(status = 'Constant') as constant
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
        sum(bad_detector) as bad_detector_count,
        sum(down_or_no_data) as down_or_no_data_count,
        sum(insufficient_data) as insufficient_data_count,
        sum(card_off) as card_off_count,
        sum(high_val) as high_val_count,
        sum(intermittent) as intermittent_count,
        sum(constant) as constant_count
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
),

detector_status_by_station_with_metadatac as (
    {{ get_county_name('detector_status_by_station_with_metadata') }}
)

select * from detector_status_by_station_with_metadatac

{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}

with

detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
),

dmeta as (
    select * from {{ ref('int_vds__detector_config') }}
),

detector_status_with_metadata as (
    select
        ds.*,
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
    from detector_status as ds
    inner join dmeta
        on
            ds.station_id = dmeta.station_id
            and ds.lane = dmeta.lane
            and ds.sample_date >= dmeta._valid_from
            and (
                ds.sample_date < dmeta._valid_to
                or dmeta._valid_to is null
            )
    where ds.sample_date is not null
)

select * from detector_status_with_metadata

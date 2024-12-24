{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
    unload_filter="sample_date >= date_trunc(month, dateadd(day, -2, current_date()))"
) }}

with

detector_status as (
    select * from {{ ref("int_diagnostics__detector_status") }}
    where sample_date is not null and lane is not null
),

detector_statusc as (
    {{ get_county_name('detector_status') }}
)

select * from detector_statusc

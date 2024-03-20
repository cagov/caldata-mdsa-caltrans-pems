with station_meta as (
    select * from {{ ref("stg_clearinghouse__station_meta") }}
),

meta_dates as (
    select distinct
        district,
        meta_date
    from station_meta
),

validity_dates as (
    select
        district,
        meta_date,
        meta_date as _valid_from,
        lead(meta_date) over (partition by district order by meta_date asc) as _valid_to
    from meta_dates
),

station_meta_scd as (
    select
        station_meta.*,
        validity_dates._valid_from,
        validity_dates._valid_to
    from station_meta
    inner join validity_dates
        on
            station_meta.meta_date = validity_dates.meta_date
            and station_meta.district = validity_dates.district
)

select * from station_meta_scd

with station_status as (
    select * from {{ ref("stg_clearinghouse__station_status") }}
),

status_dates as (
    select distinct
        district,
        meta_date
    from station_status
),

validity_dates as (
    select
        district,
        meta_date,
        meta_date as _valid_from,
        lead(meta_date) over (partition by district order by meta_date asc) as _valid_to
    from status_dates
),

station_status_scd as (
    select
        station_status.*,
        validity_dates._valid_from,
        validity_dates._valid_to
    from station_status
    inner join validity_dates
        on
            station_status.meta_date = validity_dates.meta_date
            and station_status.district = validity_dates.district
)

select * from station_status_scd

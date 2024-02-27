{{ config(materialized="table") }}

with station_meta_dates as (
    select distinct
        meta_date,
        district
    from {{ ref("stg_clearinghouse__station_meta") }}
),

date_range as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="TO_DATE('1999-06-30')",
            end_date="CURRENT_DATE()",
        )
    }}
),

districts as (
    select distinct district from station_meta_dates order by district asc
),

date_and_district as (
    select
        date_range.date_day as calendar_date,
        districts.district
    from date_range
    cross join districts
    order by calendar_date, districts.district
),

meta_for_date as (
    select
        date_and_district.*,
        station_meta_dates.meta_date
    from date_and_district
    left join station_meta_dates
        on
            date_and_district.district = station_meta_dates.district
            and date_and_district.calendar_date >= station_meta_dates.meta_date
    qualify
        row_number() over (
            partition by date_and_district.calendar_date, date_and_district.district
            order by station_meta_dates.meta_date desc nulls last
        ) = 1
    order by date_and_district.calendar_date, date_and_district.district
)

select * from meta_for_date

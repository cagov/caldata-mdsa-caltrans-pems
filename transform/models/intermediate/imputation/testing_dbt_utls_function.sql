{{ config(
    materialized="table",
    snowflake_warehouse=get_snowflake_warehouse(size="XL")
) }}

-- Use dbt_utils.date_spine to generate dates
with date_sequence as (
    select
        date_day as base_date
    from 
        {{ dbt_utils.date_spine(
            datepart="month",
            start_date="'1998-10-01'",
            end_date="current_date()"
        ) }}
    where 
        extract(day from date_day) = 3
        and extract(month from date_day) in (2, 5, 8, 11)
),

-- Select distinct regression dates
regression_dates as (
    select distinct base_date as regression_date
    from date_sequence
    order by base_date
)

-- Output regression_dates
select * from regression_dates

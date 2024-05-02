with date_range as (
        {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2010', 'mm/dd/yyyy')",
        end_date= "current_date + 1 "
        )
        }}
),

date_range_updated as (
    select date_{{ "day" }} as active_date from date_range
),

station_meta as (
    select * from {{ ref("int_clearinghouse__station_meta") }}
),

active_station as (
    select
        dr.*,
        sm.*,
        coalesce(sm._valid_to, current_date + 1) as valid_to
        --valid_to is used to place an actual date that can be used in
        --the on/between statement below. A NULL value in the _valid_to
        --field is typically associated with a currently active station
        --but is not a valid value in this CTE
    from date_range_updated as dr
    inner join
        station_meta as sm
        on
            dr.active_date between sm._valid_from and dateadd(day, -1, valid_to)
)

select * from active_station

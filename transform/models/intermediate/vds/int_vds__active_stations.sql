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
    select * from {{ ref("int_vds__station_config") }}
),

active_station as (
    select
        dr.*,
        sm.*
    from date_range_updated as dr
    inner join
        station_meta as sm
        on
            dr.active_date >= sm._valid_from
            and (dr.active_date < sm._valid_to or sm._valid_to is null)
            and sm.status = 1
)

select * from active_station

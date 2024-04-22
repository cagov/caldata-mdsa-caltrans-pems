with

recursive date_range as (
    select dateadd(day, {{ var("dev_model_look_back", current_date) }}, current_date) as active_date
    union all
    select dateadd(day, 1, active_date) as active_date
    from date_range
    where active_date < current_date
),

station_meta as (
    select * from {{ ref("int_clearinghouse__station_meta") }}
),

active_station as (
    select
        dr.*,
        sm.*,
        coalesce(sm._valid_to, current_date + 1) as valid_to
    from date_range as dr
    inner join
        station_meta as sm
        on
            dr.active_date between sm._valid_from and dateadd(day, -1, valid_to)
)

select * from active_station

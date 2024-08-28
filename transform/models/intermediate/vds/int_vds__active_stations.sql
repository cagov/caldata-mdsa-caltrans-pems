with date_range as (
        {{ dbt_utils.date_spine(
        datepart="day",
        start_date= var("pems_clearinghouse_start_date"),
        end_date= "current_date + 1 "
        )
        }}
),

date_range_updated as (
    select to_date(date_{{ "day" }}) as active_date from date_range
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
            {{ get_scd_2_data('dr.active_date','sm._valid_from','sm._valid_to') }}
            and sm.status = 1
)

select * from active_station

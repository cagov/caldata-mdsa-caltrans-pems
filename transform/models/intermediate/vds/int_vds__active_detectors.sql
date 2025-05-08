{{ config(event_time="active_date") }}

with date_range as (
        {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2010', 'mm/dd/yyyy')",
        end_date= "current_date + 1"
        )
        }}
),

date_range_updated as (
    select date_{{ "day" }} as active_date from date_range
),

detector_meta as (
    select * from {{ ref("int_vds__detector_config") }}
),

active_detector as (
    select
        dr.*,
        dm.*
    from date_range_updated as dr
    inner join
        detector_meta as dm
        on
            {{ get_scd_2_data('dr.active_date','dm._valid_from','dm._valid_to') }}

    where dm.status = 1
)

select * from active_detector

{{ config(materialized='table') }}

{% set start_date = "date('2024-01-01')" %}

with daily_total as (
    select
        any_value(district) as district,
        id,
        sample_date,
        sum(coalesce(flow_1, 0)) as flow_1,
        sum(coalesce(flow_2, 0)) as flow_2,
        sum(coalesce(flow_3, 0)) as flow_3,
        sum(coalesce(flow_4, 0)) as flow_4,
        sum(coalesce(flow_5, 0)) as flow_5,
        sum(coalesce(flow_6, 0)) as flow_6,
        sum(coalesce(flow_7, 0)) as flow_7,
        sum(coalesce(flow_8, 0)) as flow_8
    from {{ ref("stg_clearinghouse__station_raw") }}
    where sample_date >= {{ start_date }}
    group by id, sample_date
    order by sample_date asc
),

station_data as (
    select
        id,
        meta_date,
        freeway,
        direction,
        district,
        county,
        city,
        longitude,
        latitude,
        _valid_from,
        _valid_to
    from {{ ref("int_vds__station_meta") }}
),

totals_with_meta as (
    select
        daily_total.*,
        station_data.meta_date,
        station_data.freeway,
        station_data.direction
    from daily_total
    left join station_data
        on
            daily_total.id = station_data.id
            and daily_total.sample_date >= station_data._valid_from
            and daily_total.sample_date < coalesce(station_data._valid_to, dateadd(day, 1, current_date()))
)

select * from totals_with_meta

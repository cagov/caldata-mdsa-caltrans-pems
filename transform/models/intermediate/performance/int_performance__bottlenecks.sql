{{ config(materialized="table") }}

with

five_minute_pm as (
    select
        id,
        sample_date,
        sample_timestamp,
        speed_five_mins,
        delay_60_mph,
        coalesce(speed_five_mins < 40, false) as speed_less_than_40

    from {{ ref ("int_performance__five_min_perform_metrics") }}
),

distance as (
    select * from {{ ref ('int_performance__calculate_distance') }}
),

five_minute_agg_with_distance as (
    select
        d.*,
        f.* exclude (id)
    from distance as d
    inner join five_minute_pm as f
        on
            d._valid_from <= f.sample_date
            and d._valid_to > f.sample_date
            and d.id = f.id
),

calculate_speed_delta as (
    select
        *,
        speed_five_mins
        - lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
            as speed_delta
    from five_minute_agg_with_distance
),

bottleneck_criteria_check as (
    select
        *,
        case
            when
                speed_less_than_40 = true
                and distance_delta < 3
                and speed_delta > 19
                then 1
            else 0
        end as bottleneck
    from calculate_speed_delta
),

drop_persists_check as (
    select
        *,
        sum(bottleneck)
            over (
                partition by sample_timestamp, freeway, direction, type
                order by absolute_postmile asc rows between 6 preceding and current row
            )
            as persistent_speed_drop

    from bottleneck_criteria_check
)

select * from drop_persists_check

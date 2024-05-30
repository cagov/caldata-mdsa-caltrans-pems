with

five_minute_pm as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        speed_five_mins,
        delay_60_mph,
        case
            when speed_five_mins < 40 then True
            else False
        end as speed_less_than_40

    from {{ ref ("int_performance__five_min_perform_metrics") }}
    where
        TO_TIME(sample_timestamp) >= {{ var("day_start") }}
        and TO_TIME(sample_timestamp) <= {{ var("day_end") }}
        {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select
                    DATEADD(
                        day,
                        {{ var("incremental_model_look_back") }},
                        MAX(sample_date)
                    )
                from {{ this }}
            )
        {% endif %}
        {% if target.name != 'prd' %}
            and sample_date
            >= DATEADD('day', {{ var("dev_model_look_back") }}, CURRENT_DATE())
        {% endif %}
),

distance as (
    select * from {{ ref ('int_performance__calculate_distance') }}
),

five_minute_agg_with_distance as (
    select
        d.* exclude meta_date,
        f.* exclude (id, lane)
    from distance as d
    inner join five_minute_pm as f
        on
            d.meta_date = f.sample_date
            and d.id = f.id
            and d.lane = f.lane
),

calculate_speed_delta as (
    select
        *,
        speed_five_mins
        - LAG(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type, lane order by distance_rank)
            as speed_delta
    from five_minute_agg_with_distance
),

bottleneck_criteria_check as (
    select
        *,
        case
            when speed_less_than_40 = True
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
        sum(bottleneck) over (partition by sample_timestamp, freeway, direction, type, lane 
        order by distance_rank rows between 6 preceding and current row)
        as persistent_speed_drop
    
    from bottleneck_criteria_check
)

select * from drop_persists_check

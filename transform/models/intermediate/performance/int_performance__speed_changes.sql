with

five_minute_agg as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        speed_five_mins,
        delay_60_mph
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

distance_metrics as (
    select * from {{ ref ('int_performance__distance_rank') }}
),

five_minute_agg_with_distance as (
    select
        dm.*,
        fma.* exclude (id)
    from distance_metrics as dm
    inner join five_minute_agg as fma
        on
            dm.active_date = fma.sample_date
            and dm.id = fma.id
),

calculate_speed_delta as (
    select
        *,
        speed_five_mins
        - lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type, lane order by distance_rank)
            as speed_delta_from_previous
    from five_minute_agg_with_distance
)

select * from calculate_speed_delta

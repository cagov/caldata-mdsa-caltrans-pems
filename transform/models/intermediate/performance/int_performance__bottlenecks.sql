{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with

five_minute_pm as (
    select
        id,
        sample_date,
        sample_timestamp,
        -- will need to update, this is speed agg'd at the lane level and we need it at the station level
        speed_five_mins,
        delay_60_mph

    from {{ ref ("int_performance__five_min_perform_metrics") }}
    {% if is_incremental() %}
        -- Look back to account for any late-arriving data
        where
            speed_five_mins is not null and
            sample_date > (
                select
                    dateadd(
                        day,
                        {{ var("incremental_model_look_back") }},
                        max(sample_date)
                    )
                from {{ this }}
            )
            {% if target.name != 'prd' %}
                and sample_date >= (
                    dateadd(
                        day,
                        {{ var("dev_model_look_back") }},
                        current_date()
                    )
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where sample_date >= dateadd(day, {{ var("dev_model_look_back") }}, current_date())
    {% endif %}

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

speed_calcs as (
    select
        *,
        iff(direction = 'N' or direction = 'E', 
        lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type order by sample_timestamp, freeway, direction, type, absolute_postmile asc), 
        lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type order by sample_timestamp, freeway, direction, type, absolute_postmile desc))
            as speed_prev,
        iff(direction = 'N' or direction = 'E',
        speed_five_mins - lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type order by sample_timestamp, freeway, direction, type, absolute_postmile asc), 
        speed_five_mins - lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type order by sample_timestamp, freeway, direction, type, absolute_postmile desc))
            as speed_delta
    from five_minute_agg_with_distance    
),

bottleneck_start_check_ne as (
    select
        *,
        case
            when
                speed_five_mins < 40
                and distance_delta < 3
                and speed_delta <= -20
                and speed_prev >= 40
                then 1
            else 0
        end as bottleneck_start
    from speed_calcs
    where direction = 'N' or direction = 'E'
    order by sample_timestamp, freeway, direction, type, absolute_postmile asc
),

bottleneck_start_check_sw as (
    select
        *,
        case
            when
                speed_five_mins < 40
                and distance_delta < 3
                and speed_delta <= -20
                and speed_prev >= 40
                then 1
            else 0
        end as bottleneck_start
    from speed_calcs
    where direction = 'S' or direction = 'W'
    order by sample_timestamp, freeway, direction, type, absolute_postmile desc
),

unioned_data as (
select * from bottleneck_start_check_ne
union all
select * from bottleneck_start_check_sw
), 

bottleneck_persists_check as (
    select
        *,
        sum(bottleneck_start) over (
            partition by sample_timestamp, freeway, direction, type order by sample_timestamp asc rows between current row and 6 following
            ) as bottleneck_sum
    from unioned_data
)
        
select 
    *,
    iff(bottleneck_sum >= 5, true, false) as is_bottleneck
 from bottleneck_persists_check

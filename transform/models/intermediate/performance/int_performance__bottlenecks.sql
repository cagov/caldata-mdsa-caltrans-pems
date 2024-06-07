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
        delay_60_mph,
        coalesce(speed_five_mins < 40, false) as speed_less_than_40

    from {{ ref ("int_performance__five_min_perform_metrics") }}
    {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            where sample_date > (
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
            >= dateadd('day', {{ var("dev_model_look_back") }}, current_date())
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

calculate_speed_delta as (
    select
        *,
        speed_five_mins
        - lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
            as speed_delta
    from five_minute_agg_with_distance

    -- right now this is look at speed drop between adjacent stations
    -- we want to look at speed drops between the start of the bottleneck and the end
    -- find the initial 20mph drop (when the bottleneck starts), not a constant drop (maybe ranking??) 
    -- e.g i am going 60 then next data point is 40 mph - trigger that as the start. then to identify the 
    -- rest of the bottleneck check if still going 40mph or less for 5 out of 7 data points
    -- delta of 20 and less than 40 at that station is the start, look at the previous 5min am i still going 
    -- 40 and still within 3 miles that captures the length
    -- check that their still going less than or equal to speed at start of bottleneck as soon as they go over that speed 
    -- e.g. 40 mph they are out of the bottleneck
    -- aggregate bottleneck over space and time to get extent (length) and duration
),

bottleneck_criteria_check as (
    select
        *,
        case
            when
                speed_less_than_40 = true
                and distance_delta < 3
                and speed_delta <= -20
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

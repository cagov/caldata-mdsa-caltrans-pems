{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting the first day of the week date, and week number
        -- reference: https://docs.snowflake.com/en/sql-reference/functions-date-time#label-calendar-weeks-weekdays
        weekofyear(sample_date) as sample_week,
        date_trunc('week', sample_date) as sample_week_start_date
    from {{ ref('int_performance__detector_metrics_agg_daily') }}
    -- we do not want to calculate incomplete week aggregation
    where date_trunc(week, sample_date) != date_trunc(week, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly
weekly_spatial_temporal_metrics as (
    select
        detector_id,
        sample_week,
        sample_week_start_date,
        any_value(station_id) as station_id,
        any_value(lane) as lane,
        any_value(city) as city,
        any_value(county) as county,
        any_value(district) as district,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(daily_volume) as weekly_volume,
        avg(daily_occupancy) as weekly_occupancy,
        sum(daily_vmt) as weekly_vmt,
        sum(daily_vht) as weekly_vht,
        weekly_vmt / nullifzero(weekly_vht) as weekly_q_value,
        -- travel time
        60 / nullifzero(weekly_q_value) as weekly_tti,
        {% for value in var("V_t") %}
            sum(delay_{{ value }}_mph)
                as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %},
        {% for value in var("V_t") %}
            sum(lost_productivity_{{ value }}_mph)
                as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}
    from station_daily_data
    group by
        detector_id, sample_week, sample_week_start_date
)

select * from weekly_spatial_temporal_metrics

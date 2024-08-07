{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting the first day of each month
        -- reference: https://docs.snowflake.com/en/sql-reference/functions/year
        date_trunc(month, sample_date) as sample_month
    from {{ ref('int_performance__detector_metrics_agg_daily') }}
    -- # we do not want to aggregate incomplete month data
    where date_trunc(month, sample_date) != date_trunc(month, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly
monthly_spatial_temporal_metrics as (
    select
        detector_id,
        sample_month,
        any_value(station_id) as station_id,
        any_value(lane) as lane,
        any_value(city) as city,
        any_value(county) as county,
        any_value(district) as district,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(daily_volume) as monthly_volume,
        avg(daily_occupancy) as monthly_occupancy,
        sum(daily_vmt) as monthly_vmt,
        sum(daily_vht) as monthly_vht,
        monthly_vmt / nullifzero(monthly_vht) as monthly_q_value,
        -- travel time
        60 / nullifzero(monthly_q_value) as monthly_tti,
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
    group by detector_id, sample_month
)

select * from monthly_spatial_temporal_metrics

{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting first day of each month
        -- reference: https://docs.snowflake.com/en/sql-reference/functions/year
        date_trunc(month, sample_date) as sample_month
    from {{ ref('int_performance__station_metrics_agg_daily') }}
    where date_trunc(month, sample_date) != date_trunc(month, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly.
monthly_station_level_spatial_temporal_metrics as (
    select
        station_id,
        length,
        sample_month,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(daily_volume) as monthly_volume,
        avg(daily_occupancy) as monthly_occupancy,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as monthly_speed,
        sum(daily_vmt) as monthly_vmt,
        sum(daily_vht) as monthly_vht,
        monthly_vmt / nullifzero(monthly_vht) as monthly_q_value,
        -- travel time
        60 / nullifzero(monthly_q_value) as monthly_tti,
        {% for value in var("V_t") %}
            greatest(
                monthly_volume
                * ((length / nullifzero(monthly_speed)) - (length / {{ value }})),
                0
            )
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
    group by station_id, sample_month, length
)

select * from monthly_station_level_spatial_temporal_metrics

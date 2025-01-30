{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

unpivot_combined as (
    select
        city,
        sample_date,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        {% for value in var("V_t") %}
            select
                city,
                sample_date,
                '{{ value }}' as target_speed,
                nullif(delay_{{ value }}_mph, 0) as delay,
                nullif(lost_productivity_{{ value }}_mph, 0) as lost_productivity
            from
                station_daily_data
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        city, sample_date, target_speed
)

select * from unpivot_combined

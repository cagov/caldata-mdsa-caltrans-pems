{{ config(materialized='table') }}

-- read the volume, occupancy and speed weekly level data
with station_weekly_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_weekly') }}
),

weeklyc as (
    {{ get_city_name('station_weekly_data') }}
),

unpivot_combined as (
    select
        city,
        city_abb,
        city_name,
        sample_week,
        sample_week_start_date,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        {% for value in var("V_t") %}
            select
                city,
                city_abb,
                city_name,
                sample_week,
                sample_week_start_date,
                '{{ value }}' as target_speed,
                nullif(delay_{{ value }}_mph, 0) as delay,
                nullif(lost_productivity_{{ value }}_mph, 0) as lost_productivity
            from
                weeklyc
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        city, city_abb, city_name, sample_week, sample_week_start_date, target_speed
)

select * from unpivot_combined

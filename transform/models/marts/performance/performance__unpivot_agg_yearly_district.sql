{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        year(sample_date) as sample_year
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

-- aggregate delay and productivity by sample year
spatial_metrics as (
    select
        district,
        sample_year,
        {% for value in var("V_t") %}
            sum(delay_{{ value }}_mph) as delay_{{ value }}_mph,
            sum(lost_productivity_{{ value }}_mph) as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from station_daily_data
    group by
        district, sample_year
),

unpivot_combined as (
    select
        district,
        sample_year,
        target_speed,
        sum(delay) as delay,
        sum(lost_producitivity) as lost_producitivity
    from (
        {% for value in var("V_t") %}
            select
                district,
                sample_year,
                '{{ value }}' as target_speed,
                delay_{{ value }}_mph as delay,
                lost_productivity_{{ value }}_mph as lost_producitivity
            from
                spatial_metrics
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        district, sample_year, target_speed
)

select * from unpivot_combined

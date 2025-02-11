{{ config(materialized='table') }}

with monthly as (
    select * from {{ ref('int_performance__station_metrics_agg_monthly') }}
),

monthlyc as (
    {{ get_county_name('monthly') }}
),

monthlycc as (
    {{ get_city_name('monthlyc') }}
),

unpivot_combined as (
    select
        station_id,
        sample_month,
        length,
        station_type,
        district,
        city,
        city_abb,
        city_name,
        freeway,
        direction,
        county,
        county_abb,
        county_name,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        {% for value in var("V_t") %}
            select
                station_id,
                sample_month,
                length,
                station_type,
                district,
                city,
                city_abb,
                city_name,
                freeway,
                direction,
                county,
                county_abb,
                county_name,
                '{{ value }}' as target_speed,
                delay_{{ value }}_mph as delay,
                lost_productivity_{{ value }}_mph as lost_productivity
            from
                monthlycc
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        sample_month,
        station_id,
        length,
        station_type,
        district,
        city,
        city_abb,
        city_name,
        freeway,
        direction,
        county,
        county_abb,
        county_name,
        target_speed
)

select * from unpivot_combined

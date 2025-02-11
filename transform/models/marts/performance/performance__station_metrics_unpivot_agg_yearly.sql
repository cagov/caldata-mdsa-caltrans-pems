{{ config(materialized='table') }}

with yearly as (
    select * from {{ ref('int_performance__station_metrics_agg_yearly') }}
),

yearlyc as (
    {{ get_county_name('yearly') }}
),

yearlycc as (
    {{ get_city_name('yearlyc') }}
),

unpivot_combined as (
    select
        station_id,
        sample_year,
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
                sample_year,
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
                yearlycc
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        sample_year,
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

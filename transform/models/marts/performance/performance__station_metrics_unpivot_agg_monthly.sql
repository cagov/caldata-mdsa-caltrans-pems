with monthly as (
    select * from {{ ref('int_performance__station_metrics_agg_monthly') }}
),

monthlyc as (
    {{ get_county_name('monthly') }}
),

unpivot_combined as (
    select
        station_id,
        sample_month,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        county,
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
                freeway,
                direction,
                county,
                '{{ value }}' as target_speed,
                delay_{{ value }}_mph as delay,
                lost_productivity_{{ value }}_mph as lost_productivity
            from
                monthlyc
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        sample_month, station_id, length, station_type, district, city, freeway, direction, county, target_speed
)

select * from unpivot_combined

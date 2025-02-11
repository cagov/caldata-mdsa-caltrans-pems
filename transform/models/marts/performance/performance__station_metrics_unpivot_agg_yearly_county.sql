{{ config(materialized='table') }}

-- read the volume, occupancy and speed yearly level data
with station_yearly_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_yearly') }}
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        county,
        sample_year,
        {% for value in var("V_t") %}
            sum(delay_{{ value }}_mph) as delay_{{ value }}_mph,
            sum(lost_productivity_{{ value }}_mph) as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from station_yearly_data
    group by
        county, sample_year
),

unpivot_metrics as (
    {{ get_county_name('spatial_metrics') }}
),

unpivot_combined as (
    select
        county,
        sample_year,
        target_speed,
        sum(coalesce(delay, 0)) as delay,
        sum(coalesce(lost_productivity, 0)) as lost_productivity
    from (
        {% for value in var("V_t") %}
            select
                county,
                sample_year,
                '{{ value }}' as target_speed,
                nullif(delay_{{ value }}_mph, 0) as delay,
                nullif(lost_productivity_{{ value }}_mph, 0) as lost_productivity
            from
                unpivot_metrics
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        county, sample_year, target_speed
),

unpivot_combinedc as (
    {{ get_county_name('unpivot_combined') }}
)

select * from unpivot_combinedc

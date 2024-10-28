{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        year(sample_date) as sample_year
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        county,
        sample_year,
        sum(daily_volume) as yearly_volume_sum,
        avg(daily_occupancy) as yearly_occupancy_avg,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as yearly_speed_avg,
        sum(daily_vmt) as yearly_vmt,
        sum(daily_vht) as yearly_vht,
        yearly_vmt / nullifzero(yearly_vht) as yearly_q_value,
        -- travel time
        60 / nullifzero(yearly_q_value) as yearly_tti,
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
        county, sample_year
),

spatial_metricsc as (
    {{ get_county_name('spatial_metrics') }}
)

select * from spatial_metricsc

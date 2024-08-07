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
        station_id,
        sample_year,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(daily_volume) as yearly_volume,
        avg(daily_occupancy) as yearly_occupancy,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as yearly_speed,
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
        station_id, sample_year, freeway, station_type, direction
)

select * from spatial_metrics

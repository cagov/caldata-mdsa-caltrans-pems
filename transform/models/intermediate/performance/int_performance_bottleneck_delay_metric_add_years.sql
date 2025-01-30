{{ config(materialized='table') }}

-- Read the volume, occupancy, and speed daily-level data
with station_daily_data as (
    select
        *,
        -- Extracting the first day of each year
        date_trunc(year, sample_date) as sample_year
    from {{ ref('int_performance__detector_metrics_agg_daily') }}
    -- We do not want to aggregate incomplete year data
    where date_trunc(year, sample_date) != date_trunc(year, current_date)
),

-- Now aggregate daily volume, occupancy, and speed to yearly
yearly_spatial_temporal_metrics as (
    select
        detector_id,
        sample_year,
        any_value(station_id) as station_id,
        any_value(lane) as lane,
        any_value(city) as city,
        any_value(county) as county,
        any_value(district) as district,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(daily_volume) as yearly_volume,
        avg(daily_occupancy) as yearly_occupancy,
        sum(daily_vmt) as yearly_vmt,
        sum(daily_vht) as yearly_vht,
        yearly_vmt / nullifzero(yearly_vht) as yearly_q_value,
        -- Travel time
        60 / nullifzero(yearly_q_value) as yearly_tti,
        {% for value in var("V_t") %}
            sum(delay_{{ value }}_mph)
                as delay_{{ value }}_mph
            {% if not loop.last %} , {% endif %}
        {% endfor %},
        {% for value in var("V_t") %}
            sum(lost_productivity_{{ value }}_mph)
                as lost_productivity_{{ value }}_mph
            {% if not loop.last %} , {% endif %}
        {% endfor %}
    from station_daily_data
    group by detector_id, sample_year
)

select * from yearly_spatial_temporal_metrics

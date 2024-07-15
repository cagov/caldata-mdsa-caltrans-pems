{{ config(materialized='table') }}

-- read the volume, occupancy and speed hourly data
with station_hourly_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_hourly') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_station_level_spatial_temporal_agg as (
    select
        station_id,
        sample_date,
        length,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        sum(hourly_volume) as daily_volume,
        avg(hourly_occupancy) as daily_occupancy,
        sum(hourly_volume * hourly_speed) / nullifzero(sum(hourly_volume)) as daily_speed,
        sum(hourly_vmt) as daily_vmt,
        sum(hourly_vht) as daily_vht,
        daily_vmt / nullifzero(daily_vht) as daily_q_value,
        -- travel time
        60 / nullifzero(daily_q_value) as daily_tti,
        {% for value in var("V_t") %}
            greatest(
                daily_volume * ((length / nullifzero(daily_speed)) - (length / {{ value }})), 0
            )
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
    from station_hourly_data
    group by station_id, sample_date, length
)

select * from daily_station_level_spatial_temporal_agg

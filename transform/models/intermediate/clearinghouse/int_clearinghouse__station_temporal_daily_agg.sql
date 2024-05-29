{{ config(materialized='table') }}

-- read the volume, occupancy and speed hourly data
with station_hourly_data as (
    select
        *,
        cast(sample_hour as date) as sample_date
    from {{ ref('int_clearninghouse__station_temporal_hourly_agg') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_station_level_spatial_temporal_metrics as (
    select
        id,
        sample_date,
        city,
        county,
        district,
        type,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * hourly_speed) / nullifzero(sum(volume_sum)) as daily_speed,
        sum(hourly_vmt) as daily_vmt,
        sum(hourly_vht) as daily_vht,
        daily_vmt / nullifzero(daily_vht) as daily_q_value,
        -- travel time
        60 / nullifzero(daily_q_value) as daily_tti,
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
    from station_hourly_data
    group by id, sample_date, city, county, district, type
)

select * from daily_station_level_spatial_temporal_metrics

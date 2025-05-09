{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="sample_date",
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}
-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (
    select
        *,
        date_trunc('hour', sample_timestamp) as sample_timestamp_trunc
    from {{ ref('int_performance__detector_metrics_agg_five_minutes') }}
),

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_station_temporal_metrics as (
    select
        station_id,
        sample_date,
        sample_timestamp_trunc as sample_hour,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(length) as length,
        sum(volume_sum) as hourly_volume,
        avg(occupancy_avg) as hourly_occupancy,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as hourly_speed,
        sum(vmt) as hourly_vmt,
        sum(vht) as hourly_vht,
        hourly_vmt / nullifzero(hourly_vht) as hourly_q_value,
        -- travel time
        60 / nullifzero(hourly_q_value) as hourly_tti,
        {% for value in var("V_t") %}
            greatest(
                hourly_volume * ((any_value(length) / nullifzero(hourly_speed)) - (any_value(length) / {{ value }})), 0
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
    from station_five_mins_data
    group by station_id, sample_date, sample_hour
)

select * from hourly_station_temporal_metrics

{{ config(
    materialized="incremental",
    unique_key=["detector_id", "sample_date"],
    on_schema_change="sync_all_columns",
) }}

-- read the station hourly data
with station_hourly_data as (
    select *
    from {{ ref('int_performance__detector_metrics_agg_hourly') }}
    where {{ make_model_incremental('sample_date') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_spatial_temporal_agg as (
    select
        detector_id,
        sample_date,
        any_value(station_id) as station_id,
        any_value(station_type) as station_type,
        any_value(lane) as lane,
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
    group by detector_id, sample_date
)

select * from daily_spatial_temporal_agg

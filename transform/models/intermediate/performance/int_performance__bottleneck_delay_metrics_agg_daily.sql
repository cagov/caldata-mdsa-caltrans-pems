{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="sample_date",
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

with hourly_spatial_bottleneck_delay_metrics as (
    select *
    from {{ ref('int_performance__bottleneck_delay_metrics_agg_hourly') }}
),

/*aggregate hourly delay and bottleneck extent in a daily level. Since one day has
3 time shifts, the aggregation would be in a time shift level*/

daily_time_shift_spatial_bottleneck_delay_metrics as (
    select
        station_id,
        sample_date,
        time_shift,
        any_value(district) as district,
        any_value(county) as county,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(absolute_postmile) as absolute_postmile,
        sum(hourly_duration) as daily_time_shift_duration,
        avg(hourly_bottleneck_extent) as daily_time_shift_bottleneck_extent,
        -- spatial delay aggregation in daily level, decomposed into time shift
        {% for value in var("V_t") %}
            sum(hourly_spatial_delay_{{ value }}_mph)
                as daily_time_shift_spatial_delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from hourly_spatial_bottleneck_delay_metrics
    where time_shift is not NULL
    group by station_id, sample_date, time_shift
)

select * from daily_time_shift_spatial_bottleneck_delay_metrics

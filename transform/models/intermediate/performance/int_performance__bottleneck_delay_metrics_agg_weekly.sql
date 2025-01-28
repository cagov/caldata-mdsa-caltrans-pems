{{ config(
    materialized='table',
    unload_partitioning="('year=' || to_varchar(date_part('year', sample_date)) || '/week=' || to_varchar(date_part('week', sample_date)))",
) }}

with station_daily_data as (
    select
        *,
        -- Extracting the start of each week
        date_trunc(week, sample_date) as sample_week
    from {{ ref('int_performance__bottleneck_delay_metrics_agg_daily') }}
    where date_trunc(week, sample_date) != date_trunc(week, current_date)
),

weekly_spatial_bottleneck_delay_metrics as (
    select
        station_id,
        sample_week,
        time_shift,
        any_value(district) as district,
        any_value(county) as county,
        any_value(station_type) as station_type,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(absolute_postmile) as absolute_postmile,
        avg(daily_time_shift_duration) as weekly_time_shift_duration,
        sum(case when daily_time_shift_duration > 0 then 1 else 0 end) as weekly_active_days,
        avg(daily_time_shift_bottleneck_extent) as weekly_time_shift_extent,
        -- Spatial delay aggregation at weekly level, decomposed into time shift
        {% for value in var("V_t") %}
            sum(daily_time_shift_spatial_delay_{{ value }}_mph)
                as weekly_time_shift_spatial_delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from station_daily_data
    group by
        station_id,
        sample_week,
        time_shift,
        district,
        county,
        station_type,
        freeway,
        direction,
        absolute_postmile
)

select * from weekly_spatial_bottleneck_delay_metrics

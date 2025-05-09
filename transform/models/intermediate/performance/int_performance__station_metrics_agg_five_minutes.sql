{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="sample_date",
    cluster_by=["sample_date"],
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

with detector_agg_five_minutes as (
    select *
    from {{ ref('int_performance__detector_metrics_agg_five_minutes') }}
),

station_aggregated as (
    select
        station_id,
        sample_date,
        sample_timestamp,
        any_value(absolute_postmile) as absolute_postmile,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        any_value(city) as city,
        any_value(length) as length,
        round(sum(vmt), 1) as vmt,
        round(sum(vht), 2) as vht,
        sum(sample_ct) as sample_ct,
        round(sum(volume_sum), 0) as volume_sum,
        round(avg(occupancy_avg), 4) as occupancy_avg,
        round(sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)), 1) as speed_five_mins,
        {% for value in var("V_t") %}
            round(sum(lost_productivity_{{ value }}_mph), 2) as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from detector_agg_five_minutes
    group by station_id, sample_date, sample_timestamp
),

station_aggregated_with_delay as (
    select
        *,
        {% for value in var("V_t") %}
            round(greatest(volume_sum * ((length / nullifzero(speed_five_mins)) - (length / {{ value }})), 0), 2)
                as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from station_aggregated
)

select * from station_aggregated_with_delay

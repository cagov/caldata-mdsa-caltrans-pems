{{ config(materialized='table') }}

-- read the volume, occupancy and speed hourly data
with station_hourly_data as (
    select *
    from {{ ref('int_clearninghouse__station_temporal_hourly_agg') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_station_level_spatial_temporal_agg as (
    select
        id,
        sample_date,
        length,
        type,
        district,
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
    group by id, sample_date, length, type, district
),

daily_station_spatial_temporal_metrics as (
    select
        dst.*,
        sm.city,
        sm.county,
        sm.freeway,
        sm.direction
    from daily_station_level_spatial_temporal_agg as dst
    inner join {{ ref ('int_clearinghouse__station_meta') }} as sm
        on
            dst.id = sm.id
            and dst.sample_date >= sm._valid_from
            and
            (
                dst.sample_date < sm._valid_to
                or sm._valid_to is null
            )
)

select * from daily_station_spatial_temporal_metrics

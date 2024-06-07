{{ config(materialized='table') }}

-- read the station hourly data
with station_hourly_data as (
    select *
    from {{ ref('int_clearinghouse__temporal_hourly_agg') }}
),

-- now aggregate hourly volume, occupancy and speed to daily level
daily_spatial_temporal_agg as (
    select
        id,
        lane,
        sample_date,
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
    group by id, sample_date, lane
),

daily_spatial_temporal_metrics as (
    select
        dst.*,
        sm.type,
        sm.district,
        sm.city,
        sm.county,
        sm.freeway,
        sm.direction,
        sm._valid_from as station_valid_from,
        sm._valid_to as station_valid_to
    from daily_spatial_temporal_agg as dst
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

select * from daily_spatial_temporal_metrics

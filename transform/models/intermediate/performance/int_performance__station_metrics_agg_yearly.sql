{{ config(materialized='table') }}

-- Read the monthly-level data and extract the year
with station_monthly_data as (
    select
        *,
        -- Extracting first day of each year
        date_trunc('year', sample_month) as sample_year
    from {{ ref('int_performance__station_metrics_agg_monthly') }}
    -- Exclude incomplete years
    where date_trunc('year', sample_month) != date_trunc('year', current_date)
),

-- Aggregate monthly volume, occupancy, and speed to yearly
yearly_station_level_spatial_temporal_metrics as (
    select
        station_id,
        sample_year,
        any_value(station_type) as station_type,
        any_value(district) as district,
        any_value(county) as county,
        min(city) as city,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(length) as length,

        -- Summing volume-based metrics
        sum(monthly_volume) as yearly_volume,
        avg(monthly_occupancy) as yearly_occupancy,
        sum(monthly_vmt) as yearly_vmt,
        sum(monthly_vht) as yearly_vht,

        -- Weighted average speed: sum(volume * speed) / sum(volume)
        sum(monthly_volume * monthly_speed) / nullif(sum(monthly_volume), 0) as yearly_speed,

        -- Compute Q-value and TTI safely
        sum(monthly_vmt) / nullif(sum(monthly_vht), 0) as yearly_q_value,
        -- Travel time
        60 / nullif(sum(monthly_vmt) / nullif(sum(monthly_vht), 0), 0) as yearly_tti,

        {% for value in var("V_t") %}
            greatest(
                sum(monthly_volume)
                * ((any_value(length) / nullif(sum(monthly_speed), 0)) - (any_value(length) / {{ value }})),
                0
            ) as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %},

        {% for value in var("V_t") %}
            sum(lost_productivity_{{ value }}_mph) as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}

    from station_monthly_data
    group by station_id, sample_year
)

select * from yearly_station_level_spatial_temporal_metrics

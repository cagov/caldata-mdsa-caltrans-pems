{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting the week and year
        -- reference: https://docs.snowflake.com/en/sql-reference/functions-date-time#label-calendar-weeks-weekdays
        year(sample_date) as sample_year,
        weekofyear(sample_date) as sample_week,
        date_trunc('week', sample_date) as sample_week_start_date
    from {{ ref('int_clearinghouse__station_temporal_daily_agg') }}
    -- we do not want to calculate incomplete week aggregation
    where date_trunc(week, sample_date) != date_trunc(week, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly
weekly_station_level_spatial_temporal_metrics as (
    select
        id,
        sample_year,
        sample_week,
        sample_week_start_date,
        city,
        county,
        district,
        type,
        freeway,
        direction,
        sum(daily_volume) as weekly_volume,
        avg(daily_occupancy) as weekly_occupancy,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as weekly_speed,
        sum(daily_vmt) as weekly_vmt,
        sum(daily_vht) as weekly_vht,
        avg(daily_avg_length) as weekly_avg_length,
        weekly_vmt / nullifzero(weekly_vht) as weekly_q_value,
        -- travel time
        60 / nullifzero(weekly_q_value) as weekly_tti,
        {% for value in var("V_t") %}
            greatest(
                weekly_volume * ((weekly_avg_length / nullifzero(weekly_speed)) - (weekly_avg_length / {{ value }})), 0
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
    from station_daily_data
    group by id, sample_year, sample_week, sample_week_start_date, city, county, district, type, freeway, direction
)

select * from weekly_station_level_spatial_temporal_metrics

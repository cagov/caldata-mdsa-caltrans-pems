{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        -- Extracting the month and year
        year(sample_date) as sample_year,
        month(sample_date) as sample_month
    from {{ ref('int_clearinghouse__station_temporal_daily_agg') }}
    where date_trunc(month, sample_date) != date_trunc(month, current_date)
),

-- now aggregate daily volume, occupancy and speed to weekly
monthly_station_level_spatial_temporal_metrics as (
    select
        id,
        sample_year,
        sample_month,
        city,
        county,
        district,
        type,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(daily_vmt) as monthly_vmt,
        sum(daily_vht) as monthly_vht,
        monthly_vmt / nullifzero(monthly_vht) as monthly_q_value,
        -- travel time
        60 / nullifzero(monthly_q_value) as monthly_tti,
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
    from station_daily_data
    group by id, sample_year, sample_month, city, county, district, type
)

select * from monthly_station_level_spatial_temporal_metrics

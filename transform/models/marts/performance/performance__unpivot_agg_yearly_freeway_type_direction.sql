{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        year(sample_date) as sample_year
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        sample_year,
        station_type,
        freeway,
        direction,
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
    group by
        sample_year, freeway, station_type, direction
),

-- unpivot delay first
unpivot_delay as (
    select
        sample_year,
        station_type,
        freeway,
        direction,
        regexp_substr(metric, '([0-9])+', 1, 1) as target_speed,
        value as delay
    from (
        select *
        from spatial_metrics
        unpivot (
            value for metric in (
                delay_35_mph,
                delay_40_mph,
                delay_45_mph,
                delay_50_mph,
                delay_55_mph,
                delay_60_mph
            )
        )
    )
),

unpivot_lost_productivity as (
    select
        sample_year,
        station_type,
        freeway,
        direction,
        regexp_substr(metric, '([0-9])+', 1, 1) as target_speed,
        value as lost_productivity
    from (
        select *
        from spatial_metrics
        unpivot (
            value for metric in (
                lost_productivity_35_mph,
                lost_productivity_40_mph,
                lost_productivity_45_mph,
                lost_productivity_50_mph,
                lost_productivity_55_mph,
                lost_productivity_60_mph
            )
        )
    )
),

unpivot_combined as (
    select
        d.sample_year,
        d.station_type,
        d.freeway,
        d.direction,
        d.target_speed,
        d.delay,
        lp.lost_productivity
    from
        unpivot_delay as d
    left join
        unpivot_lost_productivity as lp
        on
            d.sample_year = lp.sample_year
            and d.target_speed = lp.target_speed
            and d.station_type = lp.station_type
            and d.freeway = lp.freeway
            and d.direction = lp.direction
    order by
        d.freeway, d.sample_year
)

select * from unpivot_combined

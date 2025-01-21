with monthly as (
    select * from {{ ref('int_performance__station_metrics_agg_monthly') }}
),

monthlyc as (
    {{ get_county_name('monthly') }}
),

-- unpivot delay first
unpivot_delay as (
    select
        station_id,
        sample_month,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        county,
        regexp_substr(metric, '([0-9])+', 1, 1) as target_speed,
        value as delay
    from (
        select *
        from monthlyc
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
        station_id,
        sample_month,
        regexp_substr(metric, '([0-9])+', 1, 1) as target_speed,
        value as lost_productivity
    from (
        select *
        from monthlyc
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
        d.station_id,
        d.sample_month,
        d.length,
        d.station_type,
        d.district,
        d.city,
        d.freeway,
        d.direction,
        d.county,
        d.target_speed,
        d.delay,
        lp.lost_productivity
    from
        unpivot_delay as d
    left join
        unpivot_lost_productivity as lp
        on
            d.station_id = lp.station_id
            and d.target_speed = lp.target_speed
            and d.sample_month = lp.sample_month
    order by
        d.sample_month
)

select * from unpivot_combined

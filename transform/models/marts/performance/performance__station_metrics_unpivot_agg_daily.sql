{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}

with daily as (
    select * from {{ ref('int_performance__station_metrics_agg_daily') }}
),

dailyc as (
    {{ get_county_name('daily') }}
),

-- unpivot delay first
unpivot_delay as (
    select
        station_id,
        sample_date,
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
        from dailyc
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
        sample_date,
        regexp_substr(metric, '([0-9])+', 1, 1) as target_speed,
        value as lost_productivity
    from (
        select *
        from dailyc
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
        d.sample_date,
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
            and d.sample_date = lp.sample_date
    order by
        d.sample_date
)

select * from unpivot_combined



with daily as (
    select * from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_daily
),

dailyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            daily.* exclude (county),
            c.county
        from daily
        inner join county as c
        on daily.county = c.county_id
    )

    select * from station_with_county

)

select * from dailyc
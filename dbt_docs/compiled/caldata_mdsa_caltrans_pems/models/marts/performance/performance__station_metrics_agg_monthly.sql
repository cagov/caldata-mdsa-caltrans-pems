with monthly as (
    select * from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_monthly
),

monthlyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            monthly.* exclude (county),
            c.county
        from monthly
        inner join county as c
        on monthly.county = c.county_id
    )

    select * from station_with_county

)

select * from monthlyc
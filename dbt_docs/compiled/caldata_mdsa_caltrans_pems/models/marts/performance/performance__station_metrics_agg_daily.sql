

with daily as (
    select * from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_daily
)

select * from daily
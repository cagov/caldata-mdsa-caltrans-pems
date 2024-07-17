with monthly as (
    select * from {{ ref('int_performance__station_metrics_agg_monthly') }}
)

select * from monthly
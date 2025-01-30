with monthly as (
    select * from {{ ref('int_performance__station_metrics_agg_monthly') }}
),

monthlyc as (
    {{ get_county_name('monthly') }}
)

select * from monthlyc

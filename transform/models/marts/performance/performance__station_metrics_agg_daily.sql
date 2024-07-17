{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
)}}

with daily as (
    select * from {{ ref('int_performance__station_metrics_agg_daily') }}
)

select * from daily
{{ config(materialized='table') }}

with traffic_data as (
    select
        id,
        district,
        type,
        hourly_volume,
        sample_date,
        DATE_TRUNC('month', sample_date) as observation_month
    from {{ ref('int_clearninghouse__station_temporal_hourly_agg') }}
),

traffic_with_rank as (
    select
        id,
        district,
        type,
        hourly_volume,
        observation_month,
        RANK() over (
            partition by id, observation_month
            order by hourly_volume desc
        ) as traffic_rank,
        DATEADD(year, 1, observation_month) as kfactor_month
    from traffic_data
),

kfactor as (
    select
        id,
        district,
        type,
        observation_month,
        kfactor_month,
        hourly_volume as k_factor
    from
        traffic_with_rank
    where
        traffic_rank = 30
)

select * from kfactor

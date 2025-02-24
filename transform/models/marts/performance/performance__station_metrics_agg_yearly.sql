{{ config(materialized='table') }}

with yearly as (
    select
        station_id,
        sample_year,
        length,
        station_type,
        district,
        city,
        freeway,
        direction,
        yearly_volume,
        yearly_occupancy,
        yearly_speed,
        yearly_vmt,
        yearly_vht,
        yearly_q_value,
        yearly_tti,
        county
    from {{ ref('int_performance__station_metrics_agg_yearly') }}
),

yearlyc as (
    {{ get_county_name('yearly') }}
),

yearlycc as (
    {{ get_city_name('yearlyc') }}
)

select * from yearlycc

{{ config(materialized='table') }}

-- read the volume, occupancy and speed yearly level data
with station_yearly_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_yearly') }}
),

-- now aggregate yearly volume, occupancy and speed to yearly
spatial_metrics as (
    select
        city,
        sample_year,
        sum(yearly_volume) as yearly_volume_sum,
        avg(yearly_occupancy) as yearly_occupancy_avg,
        sum(yearly_volume * yearly_speed) / nullifzero(sum(yearly_volume)) as yearly_speed_avg,
        sum(yearly_vmt) as yearly_vmt,
        sum(yearly_vht) as yearly_vht,
        sum(yearly_vmt) / nullifzero(sum(yearly_vht)) as yearly_q_value,
        -- travel time
        60 / nullifzero(sum(yearly_q_value)) as yearly_tti
    from station_yearly_data
    where
        city is not null
    group by
        city, sample_year
),

yearlyc as (
    {{ get_city_name('spatial_metrics') }}
)

select * from yearlyc

{{ config(materialized='table') }}

-- read the volume, occupancy and speed daily level data
with station_daily_data as (
    select
        *,
        year(sample_date) as sample_year
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

-- now aggregate daily volume, occupancy and speed to yearly
spatial_metrics as (
    select
        district,
        sample_year,
        sum(daily_volume) as yearly_volume_sum,
        avg(daily_occupancy) as yearly_occupancy_avg,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as yearly_speed_avg,
        sum(daily_vmt) as yearly_vmt,
        sum(daily_vht) as yearly_vht,
        yearly_vmt / nullifzero(yearly_vht) as yearly_q_value,
        -- travel time
        60 / nullifzero(yearly_q_value) as yearly_tti
    from station_daily_data
    group by
        district, sample_year
)

select * from spatial_metrics

with station_weekly_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_weekly') }}
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        county,
        sample_week,
        sum(weekly_volume) as weekly_volume_sum,
        avg(weekly_occupancy) as weekly_occupancy_avg,
        sum(weekly_volume * weekly_speed) / nullifzero(sum(weekly_volume)) as weekly_speed_avg,
        sum(weekly_vmt) as weekly_vmt,
        sum(weekly_vht) as weekly_vht,
        sum(weekly_vmt) / nullifzero(sum(weekly_vht)) as weekly_q_value,
        60 / nullifzero(sum(weekly_q_value)) as weekly_tti
    from station_weekly_data
    group by
        county, sample_week
)

select * from spatial_metrics

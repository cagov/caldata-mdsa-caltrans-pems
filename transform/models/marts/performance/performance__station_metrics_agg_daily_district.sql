with station_daily_data as (
    select *
    from {{ ref('int_performance__station_metrics_agg_daily') }}
),

-- now aggregate daily volume, occupancy and speed to daily
spatial_metrics as (
    select
        district,
        sample_date,
        sum(daily_volume) as daily_volume_sum,
        avg(daily_occupancy) as daily_occupancy_avg,
        sum(daily_volume * daily_speed) / nullifzero(sum(daily_volume)) as daily_speed_avg,
        sum(daily_vmt) as daily_vmt,
        sum(daily_vht) as daily_vht,
        sum(daily_vmt) / nullifzero(sum(daily_vht)) as daily_q_value,
        60 / nullifzero(sum(daily_q_value)) as daily_tti
    from station_daily_data
    group by
        district, sample_date
)

select * from spatial_metrics

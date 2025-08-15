

with station_weekly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_weekly
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        city,
        sample_week,
        sum(weekly_volume) as weekly_volume_sum,
        avg(weekly_occupancy) as weekly_occupancy_avg,
        sum(weekly_volume * weekly_speed) / nullifzero(sum(weekly_volume)) as weekly_speed_avg,
        sum(weekly_vmt) as weekly_vmt,
        sum(weekly_vht) as weekly_vht,
        sum(weekly_vmt) / nullifzero(sum(weekly_vht)) as weekly_q_value,
        60 / nullifzero(sum(weekly_q_value)) as weekly_tti
    from station_weekly_data
    where
        city is not null
    group by
        city, sample_week
),

weeklyc as (
    
    with city as (
        select
            city_id,
            city_name,
            native_id
        from ANALYTICS_PRD.analytics.cities
    ),
    station_with_city_id as (
        select
            st.*,
            c.city_name,
            c.native_id as city_abb
        from spatial_metrics as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id

)

select * from weeklyc
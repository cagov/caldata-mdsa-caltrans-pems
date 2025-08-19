

with station_weekly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_weekly
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
),

weeklyc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            spatial_metrics.*,
            c.county_name,
            c.county_abb
        from spatial_metrics
        inner join county as c
        on spatial_metrics.county = c.county_id
    )

    select * from station_with_county

)

select * from weeklyc
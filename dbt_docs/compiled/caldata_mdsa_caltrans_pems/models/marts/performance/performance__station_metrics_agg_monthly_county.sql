

with station_monthly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_monthly
),

-- now aggregate daily volume, occupancy and speed to monthly
spatial_metrics as (
    select
        county,
        sample_month,
        sum(monthly_volume) as monthly_volume_sum,
        avg(monthly_occupancy) as monthly_occupancy_avg,
        sum(monthly_volume * monthly_speed) / nullifzero(sum(monthly_volume)) as monthly_speed_avg,
        sum(monthly_vmt) as monthly_vmt,
        sum(monthly_vht) as monthly_vht,
        sum(monthly_vmt) / nullifzero(sum(monthly_vht)) as monthly_q_value,
        60 / nullifzero(sum(monthly_q_value)) as monthly_tti
    from station_monthly_data
    group by
        county, sample_month
),

monthlyc as (
    
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

select * from monthlyc
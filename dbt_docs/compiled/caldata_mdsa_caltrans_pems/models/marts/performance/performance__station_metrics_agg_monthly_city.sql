

with station_monthly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_monthly
),

-- now aggregate daily volume, occupancy and speed to weekly
spatial_metrics as (
    select
        city,
        sample_month,
        sum(monthly_volume) as monthly_volume_sum,
        avg(monthly_occupancy) as monthly_occupancy_avg,
        sum(monthly_volume * monthly_speed) / nullifzero(sum(monthly_volume)) as monthly_speed_avg,
        sum(monthly_vmt) as monthly_vmt,
        sum(monthly_vht) as monthly_vht,
        sum(monthly_vmt) / nullifzero(sum(monthly_vht)) as monthly_q_value,
        60 / nullifzero(sum(monthly_q_value)) as monthly_tti
    from station_monthly_data
    where
        city is not null
    group by
        city, sample_month
),

monthlyc as (
    
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

select * from monthlyc
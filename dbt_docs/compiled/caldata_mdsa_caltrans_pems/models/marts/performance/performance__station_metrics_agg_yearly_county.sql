

-- read the volume, occupancy and speed daily level data
-- read the volume, occupancy and speed yearly level data
with station_yearly_data as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_metrics_agg_yearly
),

-- now aggregate yearly volume, occupancy and speed to yearly
spatial_metrics as (
    select
        county,
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
    group by
        county, sample_year
),

spatial_metricsc as (
    
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

select * from spatial_metricsc
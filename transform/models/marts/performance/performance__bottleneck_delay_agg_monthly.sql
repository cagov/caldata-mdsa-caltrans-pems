with monthly_bottleneck_delay as (
    select *
    from {{ ref('int_performance__bottleneck_delay_metrics_agg_monthly') }}
),

bottleneck_delay_with_county as (
    {{ get_county_name('monthly_bottleneck_delay') }}
),

geo as (
    select
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location
    from {{ ref('geo__current_stations') }}
),

bottleneck_delay_county_geo as (
    select
        bottleneck_delay_with_county.*,
        geo.latitude,
        geo.longitude,
        geo.location
    from
        bottleneck_delay_with_county
    inner join
        geo
        on bottleneck_delay_with_county.station_id = geo.station_id
)

select * from bottleneck_delay_county_geo

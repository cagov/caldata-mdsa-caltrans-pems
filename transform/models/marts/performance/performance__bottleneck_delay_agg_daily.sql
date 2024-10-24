with daily_bottleneck_delay as (
    select * from {{ ref('int_performance__bottleneck_delay_metrics_agg_daily') }}
),

county as (
    select
        county_id,
        county_name
    from {{ ref('counties') }}
),

bottleneck_delay_with_county as (
    select
        daily_bottleneck_delay.* exclude (county),
        c.county_name
    from
        daily_bottleneck_delay
    inner join
        county as c
        on daily_bottleneck_delay.county = c.county_id
),

geo as (
    select distinct
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location,
    from {{ ref('geo__current_detectors') }}
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

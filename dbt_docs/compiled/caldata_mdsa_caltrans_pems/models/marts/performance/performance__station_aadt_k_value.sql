with aadt as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_aadt_with_K_value
),

county as (
    select
        county_id,
        county_name
    from ANALYTICS_PRD.clearinghouse.counties
),

aadt_with_county as (
    select
        aadt.* exclude (county),
        c.county_name
    from
        aadt
    inner join
        county as c
        on aadt.county = c.county_id
),

geo as (
    select distinct
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location,
        absolute_postmile
    from ANALYTICS_PRD.geo.geo__current_detectors
),

aadt_county_geo as (
    select
        aadt_with_county.*,
        geo.absolute_postmile,
        geo.latitude,
        geo.longitude,
        geo.location
    from
        aadt_with_county
    inner join
        geo
        on aadt_with_county.station_id = geo.station_id
)

select * from aadt_county_geo
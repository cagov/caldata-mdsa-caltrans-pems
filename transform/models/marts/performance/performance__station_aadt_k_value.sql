with aadt as (
    select *
    from {{ ref('int_performance__station_aadt_with_K_value') }}
),

county as (
    select
        county_id,
        county_name
    from {{ ref('counties') }}
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
        absolute_postmile
    from {{ ref('geo__current_detectors') }}
),

aadt_county_geo as (
    select
        aadt_with_county.*,
        geo.absolute_postmile,
        geo.latitude,
        geo.longitude
    from
        aadt_with_county
    inner join
        geo
        on aadt_with_county.station_id = geo.station_id
)

select * from aadt_county_geo

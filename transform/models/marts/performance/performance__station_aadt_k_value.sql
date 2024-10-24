with aadt as (
    select *
    from {{ ref('int_performance__station_aadt_with_K_value') }}
),

aadt_with_county as (
    {{ get_county_name('aadt') }}
),

geo as (
    select distinct
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location,
        absolute_postmile
    from {{ ref('geo__current_detectors') }}
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

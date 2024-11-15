with aadt as (
    select *
    from {{ ref('int_performance__station_aadt_with_K_value') }}
),

aadt_with_county as (
    {{ get_county_name('aadt') }}
),

geo as (
    select
        current_geo.station_id,
        current_geo.latitude,
        current_geo.longitude,
        concat(current_geo.longitude, ',', current_geo.latitude) as location,
        current_geo.absolute_postmile
    from
        {{ ref('geo__current_detectors') }} as current_geo
    where
        current_geo.absolute_postmile = (
            select max(sub.absolute_postmile)
            from {{ ref('geo__current_detectors') }} as sub
            where sub.station_id = current_geo.station_id
        )
    group by
        current_geo.station_id,
        current_geo.latitude,
        current_geo.longitude,
        current_geo.absolute_postmile
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

with aadt as (
    select *
    from ANALYTICS_PRD.performance.int_performance__station_aadt_with_K_value
),

aadt_with_county as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            aadt.*,
            c.county_name,
            c.county_abb
        from aadt
        inner join county as c
        on aadt.county = c.county_id
    )

    select * from station_with_county

),

geo as (
    select
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location,
        absolute_postmile
    from ANALYTICS_PRD.geo.geo__current_stations
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
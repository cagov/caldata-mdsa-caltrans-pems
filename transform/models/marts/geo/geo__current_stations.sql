with station_config as (
    select * from {{ ref('int_vds__station_config') }}
),

current_stations as (
    select
        * exclude (_valid_from, _valid_to),
        st_makepoint(longitude, latitude) as geometry
    from station_config
    where _valid_to is null
),

current_stationsc as (
    {{ get_county_name('current_stations') }}
),

current_stationscc as (
    {{ get_city_name('current_stationsc') }}
)

select * from current_stationscc

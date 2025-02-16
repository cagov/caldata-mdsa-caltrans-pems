with station_config as (
    select * from ANALYTICS_PRD.vds.int_vds__station_config
),

current_stations as (
    select
        * exclude (_valid_from, _valid_to),
        st_makepoint(longitude, latitude) as geometry
    from station_config
    where _valid_to is null
),

current_stationsc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            current_stations.*,
            c.county_name,
            c.county_abb
        from current_stations
        inner join county as c
        on current_stations.county = c.county_id
    )

    select * from station_with_county

)

select * from current_stationsc
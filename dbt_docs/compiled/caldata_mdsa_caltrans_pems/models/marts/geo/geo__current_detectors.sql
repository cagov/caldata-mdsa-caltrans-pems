with detector_config as (
    select * from ANALYTICS_PRD.vds.int_vds__detector_config
),

current_detectors as (
    select
        * exclude (_valid_from, _valid_to),
        st_makepoint(longitude, latitude) as geometry
    from detector_config
    where _valid_to is null
),

current_detectorsc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            current_detectors.*,
            c.county_name,
            c.county_abb
        from current_detectors
        inner join county as c
        on current_detectors.county = c.county_id
    )

    select * from station_with_county

)

select * from current_detectorsc
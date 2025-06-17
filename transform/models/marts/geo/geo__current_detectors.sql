with detector_config as (
    select * from {{ ref('int_vds__detector_config') }}
),

current_detectors as (
    select
        * exclude (_valid_from, _valid_to),
        st_makepoint(longitude, latitude) as geometry
    from detector_config
    where _valid_to is null
),

current_detectorsc as (
    {{ get_county_name('current_detectors') }}
),

current_detectorscc as (
    {{ get_city_name('current_detectorsc') }}
)

select * from current_detectorscc

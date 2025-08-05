select
    station_id,
    station_type,
    district_id as district, -- TODO: make sure this is an int
    county_id as county,
    city_id as city,
    freeway_id as freeway,
    freeway_dir as direction
from {{ source('db96', 'station_config') }}

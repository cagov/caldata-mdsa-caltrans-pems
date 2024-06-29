select
    controller_id,
    controller_type,
    district_id as district,
    county_id as county,
    city_id as city,
    freeway_id as freeway,
    freeway_dir as direction
from {{ source('db96', 'controller_config') }}

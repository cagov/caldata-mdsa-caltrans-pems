select
    station_id,
    station_type,
    district_id as district,
    county_id as county,
    city_id as city,
    freeway_id as freeway,
    freeway_dir as direction
from RAW_PRD.db96.station_config
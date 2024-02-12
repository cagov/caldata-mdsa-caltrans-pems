select
    DATE_FROM_PARTS(
        CAST(SUBSTRING(filename, 18, 4) as NUMBER),    -- Extract Year
        CAST(SUBSTRING(filename, 23, 2) as NUMBER),    -- Extract Month
        CAST(SUBSTRING(filename, 48, 2) as NUMBER)     -- Extract Day
    ) as extracted_date,
    -- filename,
    -- id,
    fwy,
    dir,
    district,
    county,
    city,
    state_pm,
    abs_pm,
    latitude,
    longitude,
    length,
    type,
    lanes,
    name
from raw_prd.clearinghouse.station_meta
limit 10000

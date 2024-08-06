select

    COUNTYFP10 as COUNTY_FIPS,
    GEOID10 as GEO_ID,
    NAME10 as NAME,
    ALAND10 as LAND,
    AWATER10 as WATER,
    INTPTLAT10 as LATITUDE,
    INTPTLON10 as LONGITUDE,
    CO_CODE as COUNTY_CODE,
    DISTRICT,
    "Shape__Area" as AREA,
    "Shape__Length" as LENGTH,
    "geometry" as GEOMETRY

from {{ source('geo_reference', 'COUNTY_BOUNDARIES') }}

select 

    DISTRICT,
    "Region" as REGION,
    "Shape__Area" as AREA,
    "Shape__Length" as LENGTH,
    "geometry" as GEOMETRY

FROM {{ source('geo_reference', 'DISTRICTS') }}
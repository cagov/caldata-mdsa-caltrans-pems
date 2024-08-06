select

    DISTRICT,
    "Region" as REGION,
    "Shape__Area" as AREA,
    "Shape__Length" as LENGTH,
    "geometry" as GEOMETRY

from {{ source('geo_reference', 'districts') }}

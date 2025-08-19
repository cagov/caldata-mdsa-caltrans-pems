select

    DISTRICT,
    "Region" as REGION,
    "Shape__Area" as AREA,
    "Shape__Length" as LENGTH,
    "geometry" as GEOMETRY

from RAW_PRD.geo_reference.districts
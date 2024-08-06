select 

    DISTRICT,
    Region,
    Shape__Area,
    Shape__Length,
    geometry

FROM {{ source('geo_reference', 'districts') }}
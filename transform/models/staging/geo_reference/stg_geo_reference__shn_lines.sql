select 

    Route,
    RteSuffix,
    RouteS,
    PMRouteID,
    County,
    District,
    PMPrefix,
    bPM,
    ePM,
    PMSuffix,
    bPMc,
    ePMc,
    bOdometer,
    eOdometer,
    AlignCode,
    RouteType,
    Direction,
    Shape__Length,
    geometry
    
FROM {{ source('geo_reference', 'shn_lines') }}
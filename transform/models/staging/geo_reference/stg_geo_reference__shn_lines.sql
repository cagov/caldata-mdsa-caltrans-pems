select 

    /* RteSuffix was dropped because less than 1% of it 
    was filled and as such it didn't seem very useful.
    Route is very similar to RouteS and was therefore dropped
    as well. */

    "RouteS" as ROUTE_WITH_SUFFIX,
    "PMRouteID" as ,
    "County",
    "District",
    "PMPrefix",
    "bPM",
    "ePM",
    "PMSuffix",
    "bPMc",
    "ePMc",
    "bOdometer",
    "eOdometer",
    "AlignCode",
    "RouteType" as ROUTE_TYPE,
    "Direction" as DIRECTION,
    "Shape__Length" as LENGTH,
    "geometry" as GEOMETRY,
    
FROM {{ source('geo_reference', 'SHN_LINES') }}
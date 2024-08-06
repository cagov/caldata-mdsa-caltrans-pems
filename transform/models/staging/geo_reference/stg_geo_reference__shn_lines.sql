select 

    /* RteSuffix was dropped because less than 1% of it 
    was filled and as such it didn't seem very useful.
    Route is very similar to RouteS and was therefore dropped
    as well. */

    "RouteS" as ROUTE_WITH_SUFFIX,
    "PMRouteID" as PM_ROUTE_ID,
    "County" as COUNTY,
    "District" as DISTRICT,
    "PMPrefix" as PM_PREFIX,
    "bPM" as BPM,
    "ePM" as EPM,
    "PMSuffix" as PM_SUFFIX,
    "bPMc" as BPMC,
    "ePMc" as EPMC,
    "bOdometer" as B_ODOMETER,
    "eOdometer" as E_ODOMETER,
    "AlignCode" as ALIGN_CODE,
    "RouteType" as ROUTE_TYPE,
    "Direction" as DIRECTION,
    "Shape__Length" as LENGTH,
    "geometry" as GEOMETRY,
    
FROM {{ source('geo_reference', 'shn_lines') }}
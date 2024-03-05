{% set
  date_re='clhouse/status/d\\\\d{2}/\\\\d{4}/\\\\d{2}/d(\\\\d{2})_tmdd_meta_(\\\\d{4})_(\\\\d{2})_(\\\\d{2}).xml'
%}
/*
Helpful article for flattening XML:
https://community.snowflake.com/s/article/HOW-TO-QUERY-NESTED-XML-DATA-IN-SNOWFLAKE
*/
SELECT
    STATUS.FILENAME AS FILENAME,
    DATE_FROM_PARTS(
        REGEXP_SUBSTR(STATUS.FILENAME, '{{ date_re }}', 1, 1, '', 2)::INT,
        REGEXP_SUBSTR(STATUS.FILENAME, '{{ date_re }}', 1, 1, '', 3)::INT,
        REGEXP_SUBSTR(STATUS.FILENAME, '{{ date_re }}', 1, 1, '', 4)::INT
    ) AS META_DATE,
    REGEXP_SUBSTR(STATUS.FILENAME, '{{ date_re }}', 1, 1, '', 1)::INT AS DISTRICT,
    XMLGET(STATUS.CONTENT, 'station-id'):"$" AS STATION_ID,
    XMLGET(XMLGET(DETECTOR.VALUE, 'detector'), 'detector-id'):"$"::VARCHAR AS DETECTOR_ID,
    XMLGET(XMLGET(DETECTOR.VALUE, 'detector'), 'detector-name'):"$"::VARCHAR AS DETECTOR_NAME,
    XMLGET(XMLGET(DETECTOR.VALUE, 'detector'), 'detector-status'):"$"::VARCHAR AS DETECTOR_STATUS,
    XMLGET(XMLGET(DETECTOR.VALUE, 'detector'), 'tmdd:last-update-time'):"$"::VARCHAR AS LAST_UPDATE_TIME,
    XMLGET(XMLGET(LANE.VALUE, 'detection-lane-item'), 'lane-number'):"$"::INT AS LANE_NUMBER
FROM
    {{ source("clearinghouse", "station_status") }} AS STATUS,
    /*
    It's not 100% clear that these flattening operations are necessary. The structure of
    the XML documents suggest that there can be multiple detectors in a 'detector-list',
    and multiple lanes in a 'detection-lane'. However, there seem to be few (if any) instances
    of entries where that is the case. So these flattenings are defensive.
    */
    LATERAL FLATTEN(STATUS.CONTENT:"$") AS DETECTOR,
    LATERAL FLATTEN(XMLGET(DETECTOR.VALUE, 'detector'):"$") AS LANE
WHERE
    GET(DETECTOR.VALUE, '@') = 'detector-list'
    AND GET(LANE.VALUE, '@') = 'detection-lane'

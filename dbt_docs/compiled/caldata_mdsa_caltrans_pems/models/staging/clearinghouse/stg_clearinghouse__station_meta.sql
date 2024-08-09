

SELECT
    FILENAME,
    DATE_FROM_PARTS(
        REGEXP_SUBSTR(FILENAME, 'clhouse/meta/d\\d{2}/\\d{4}/\\d{2}/d\\d{2}_text_meta_(\\d{4})_(\\d{2})_(\\d{2}).txt', 1, 1, '', 1)::INT,
        REGEXP_SUBSTR(FILENAME, 'clhouse/meta/d\\d{2}/\\d{4}/\\d{2}/d\\d{2}_text_meta_(\\d{4})_(\\d{2})_(\\d{2}).txt', 1, 1, '', 2)::INT,
        REGEXP_SUBSTR(FILENAME, 'clhouse/meta/d\\d{2}/\\d{4}/\\d{2}/d\\d{2}_text_meta_(\\d{4})_(\\d{2})_(\\d{2}).txt', 1, 1, '', 3)::INT
    ) AS META_DATE,
    ID,
    FWY AS FREEWAY,
    DIR AS DIRECTION,
    DISTRICT,
    COUNTY,
    CITY,
    STATE_PM AS STATE_POSTMILE,
    ABS_PM AS ABSOLUTE_POSTMILE,
    LATITUDE,
    LONGITUDE,
    LENGTH,
    TYPE,
    LANES,
    NAME
FROM RAW_PRD.clearinghouse.station_meta
{% set date_re = 'clhouse/meta/d\\\\d{2}/\\\\d{4}/\\\\d{2}/d\\\\d{2}_text_meta_(\\\\d{4})_(\\\\d{2})_(\\\\d{2}).txt' %}

SELECT
    DATE_FROM_PARTS(
        REGEXP_SUBSTR(FILENAME, '{{ date_re }}', 1, 1, '', 1)::INT,
        REGEXP_SUBSTR(FILENAME, '{{ date_re }}', 1, 1, '', 2)::INT,
        REGEXP_SUBSTR(FILENAME, '{{ date_re }}', 1, 1, '', 3)::INT
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
FROM {{ source('clearinghouse', 'station_meta') }}

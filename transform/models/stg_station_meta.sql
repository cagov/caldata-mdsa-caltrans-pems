{% set date_re = 'clhouse/meta/d\\\\d{2}/\\\\d{4}/\\\\d{2}/d\\\\d{2}_text_meta_(\\\\d{4})_(\\\\d{2})_(\\\\d{2}).txt' %}

SELECT
    * EXCLUDE (FILENAME, USER_ID_1, USER_ID_2, USER_ID_3, USER_ID_4),
    DATE_FROM_PARTS(
        REGEXP_SUBSTR(FILENAME, '{{ date_re }}', 1, 1, '', 1)::INT,
        REGEXP_SUBSTR(FILENAME, '{{ date_re }}', 1, 1, '', 2)::INT,
        REGEXP_SUBSTR(FILENAME, '{{ date_re }}', 1, 1, '', 3)::INT
    ) AS META_DATE
FROM {{ source('clearinghouse', 'STATION_META') }}

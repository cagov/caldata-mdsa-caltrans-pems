select
    ID,
    FWY as FREEWAY,
    DIR as DIRECTION,
    DISTRICT,
    COUNTY,
    CITY,
    STATE_PM as STATE_POSTMILE,
    ABS_PM as ABSOLUTE_POSTMILE,
    LATITUDE,
    LONGITUDE,
    LENGTH,
    TYPE,
    LANES as NUMBER,
    NAME,
    USER_ID_1,
    USER_ID_2,
    USER_ID_3,
    USER_ID_4,
    DATE_FROM_PARTS(SUBSTR(FILENAME, 18, 4), SUBSTR(FILENAME, 23, 2), LEFT(RIGHT(FILENAME, 6), 2)) as META_DATE
from {{ source('clearinghouse', 'station_meta') }}
limit 10
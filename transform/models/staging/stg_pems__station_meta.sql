select 
    ID,
    FWY as FREEWAY_NO,
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
    LANES,
    NAME,
    CAST(
        SUBSTRING(FILENAME, POSITION('_meta_' in FILENAME) + len('_meta_'), len('2024')) || '-'
        || SUBSTRING(FILENAME, POSITION('_meta_' in FILENAME) + len('_meta_2024_'), len('01')) || '-'
        || SUBSTRING(FILENAME, POSITION('_meta_' in FILENAME) + len('_meta_2024_01_'), len('31')) as Date
    ) as META_DATE
from {{ source('CLEARINGHOUSE', 'STATION_META') }}
-- limit 10

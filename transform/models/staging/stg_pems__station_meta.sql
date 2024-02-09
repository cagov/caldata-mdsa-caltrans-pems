select
    -- FILENAME,
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
        SUBSTRING(FILENAME, POSITION('_meta_' in FILENAME) + 6, 4) || '-'
        || SUBSTRING(FILENAME, POSITION('_meta_' in FILENAME) + 11, 2) || '-'
        || SUBSTRING(FILENAME, POSITION('_meta_' in FILENAME) + 14, 2) as Date
    ) as META_DATE
FROM raw_prd.clearinghouse.station_meta;
-- limit 10
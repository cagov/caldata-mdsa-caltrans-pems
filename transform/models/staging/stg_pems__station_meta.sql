with

source as (
    select * from {{ source('CLEARINGHOUSE', 'STATION_META') }}
),

stn_meta as (
    select
        id,
        fwy as fwy_id,
        dir as fwy_dir,
        district,
        county,
        city,
        state_pm as postmile_state,
        abs_pm as postmile_abs,
        latitude,
        longitude,
        length,
        type,
        lanes,
        name,
        CAST(
            SUBSTRING(filename, POSITION('_meta_' in filename) + 6, 4) || '-'
            || SUBSTRING(filename, POSITION('_meta_' in filename) + 11, 2) || '-'
            || SUBSTRING(filename, POSITION('_meta_' in filename) + 14, 2) as Date
        ) as meta_date
    from source
)

select * from stn_meta

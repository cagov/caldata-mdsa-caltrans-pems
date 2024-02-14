with
source as (
    select * from {{ source('CLEARINGHOUSE', 'STATION_META') }}
   -- select * from stg_pems__station_meta
),

station_meta_with_date as (
    select
        date_from_parts(
            substr(filename, 18, 4)::INT,
            substr(filename, 23, 2)::INT,
            substr(right(filename, 6), 1, 2)::INT
        ) as meta_date,
        id,
        fwy as freeway,
        dir as direction,
        district,
        county as county_fips,
        city,
        state_pm as state_postmile,
        abs_pm as absolute_postmile,
        latitude,
        longititude,
        length,
        type,
        lanes,
        name
    from source
)



select *
from station_meta_with_date
limit 10

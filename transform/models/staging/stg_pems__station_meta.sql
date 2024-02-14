with

source as (

    select * from {{ source('clearinghouse', 'STATION_META') }}

),

station_meta_with_date as (

    select
        DATE_FROM_PARTS(
            SUBSTR(filename, 18, 4)::INT,
            SUBSTR(filename, 23, 2)::INT,
            SUBSTR(RIGHT(filename, 6), 1, 2)::INT
        ) as meta_date,
        id,
        fwy as freeway,
        dir as direction,
        district,
        county as county_fips,
        {{ map_county_fips_to_county_name("COUNTY") }} as county_name,
        city,
        state_pm as state_postmile,
        abs_pm as absolute_postmile,
        latitude,
        longitude,
        length,
        type,
        lanes,
        name

    from source
)

select * from station_meta_with_date

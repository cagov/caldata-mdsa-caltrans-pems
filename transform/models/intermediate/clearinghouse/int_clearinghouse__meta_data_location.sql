{{ config(materialized="table") }}

with meta_location as (
    select distinct
        id,
        direction,
        city,
        county,
        district,
        freeway,
        lanes,
        latitude,
        longitude,
        type,
        abs_pm,
        length,
        state_pm
    from {{ ref('stg_clearinghouse__station_meta') }}
    where
        meta_date >= '2023-06-10'
        and meta_date <= '2024-06-09'
        and district not in (1, 2, 6, 9)
        and type in ('HV', 'ML')
)

select * from meta_location

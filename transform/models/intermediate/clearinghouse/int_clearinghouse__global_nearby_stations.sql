{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref('int_clearinghouse__station_meta') }}
),

global_station_pairs as (
    select
        a.id,
        b.id as other_id,
        a.district,
        a.freeway,
        a.direction,
        a.type,
        b.absolute_postmile - a.absolute_postmile as delta_postmile,
        a._valid_from,
        a._valid_to
    from station_meta as a
    inner join station_meta as b
        on
            a.freeway = b.freeway and a.direction = b.direction and a.type = b.type and a.district = b.district
            and a.meta_date = b.meta_date
)

select * from global_station_pairs

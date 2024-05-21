{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref('int_clearinghouse__most_recent_station_meta') }}
),

nearest_stations_within_beffer as (
    select
        a.id,
        b.id as other_id,
        a.district,
        a.freeway,
        a.direction,
        a.type,
        -- TODO: is this the best metric for "nearest"? State postmiles often have
        -- some string part to them, making math a little trickier.
        b.absolute_postmile - a.absolute_postmile as delta_postmile
    from station_meta as a
    inner join station_meta as b
        -- on a.freeway = b.freeway and a.direction = b.direction and a.type = b.type
        on a.freeway = b.freeway and a.direction = b.direction and a.type = b.type and a.district = b.district
    -- TODO: distance comparisons don't seem appropriate for all station types,
    -- e.g., on/off ramps. So it makes sense to restrict these comparisons to some
    -- types. Is this the right set?
    where a.type in ('HV', 'ML') and a.id != b.id and abs(delta_postmile) <= 5
    -- where a.type in ('HV', 'ML') and a.id != b.id 
)

select * from nearest_stations_within_beffer

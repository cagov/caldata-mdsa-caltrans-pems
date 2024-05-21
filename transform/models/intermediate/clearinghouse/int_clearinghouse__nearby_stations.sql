{{ config(materialized="table") }}
with station_meta as (
    select * from {{ ref('int_clearinghouse__most_recent_station_meta') }}
),

station_pairs as (
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
        on a.freeway = b.freeway and a.direction = b.direction and a.type = b.type
    -- TODO: distance comparisons don't seem appropriate for all station types,
    -- e.g., on/off ramps. So it makes sense to restrict these comparisons to some
    -- types. Is this the right set?
    where a.type in ('HV', 'ML')
),

nearest_downstream_station_pairs as (
    select
        id,
        other_id,
        district,
        freeway,
        direction,
        type,
        delta_postmile
    from station_pairs
    where
        delta_postmile > 0 and id != other_id
    qualify row_number() over (partition by id order by delta_postmile asc) = 1
),

nearest_upstream_station_pairs as (
    select *
    from station_pairs
    where
        delta_postmile < 0
        and id != other_id
    qualify row_number() over (partition by id order by abs(delta_postmile) asc) = 1
),

self_pairs as (
    select *
    from station_pairs
    where id = other_id
),

nearest_station_pairs as (
    select *
    from self_pairs
    union all
    select *
    from nearest_downstream_station_pairs
    union all
    select * from nearest_upstream_station_pairs
    order by district asc, freeway asc, id asc
)

-- select * from nearest_station_pairs
select * from station_pairs

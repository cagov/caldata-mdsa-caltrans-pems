{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref('int_clearinghouse__station_meta') }}
),

station_pairs as (
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
            a.freeway = b.freeway and a.direction = b.direction and a.type = b.type
            and a.meta_date = b.meta_date
    -- Most performance metrics are restricted to mainline and HV lanes.
    -- Furthermore, when looking at upstream and downstream stations, it
    -- does not make sense to include, e.g., ramps. So for the time being
    -- we restrict this table to HV and ML.
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
        delta_postmile,
        _valid_from,
        _valid_to,
        row_number() over (partition by id, _valid_from order by abs(delta_postmile) asc)
            as row_number
    from station_pairs
    where
        delta_postmile > 0
        and delta_postmile <= 5.0
        and id != other_id
),

nearest_upstream_station_pairs as (
    select
        id,
        other_id,
        district,
        freeway,
        direction,
        type,
        delta_postmile,
        _valid_from,
        _valid_to,
        row_number() over (partition by id, _valid_from order by abs(delta_postmile) asc)
            as row_number
    from station_pairs
    where
        delta_postmile < 0
        and delta_postmile > -5.0
        and id != other_id
),

self_pairs as (
    select
        *,
        0 as row_number
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
),

-- assign the tag that is qulified for local and regional regression
nearest_station_pairs_with_tag as (
    select
        *,
        case
            when row_number <= 1 then 1 else 0
        end as local_reg
    from nearest_station_pairs
)

select * from nearest_station_pairs_with_tag

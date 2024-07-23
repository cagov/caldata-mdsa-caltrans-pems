{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref('int_vds__station_config') }}
),

station_pairs as (
    select
        a.station_id,
        b.station_id as other_station_id,
        a.district,
        a.freeway,
        a.direction,
        a.station_type,
        b.absolute_postmile - a.absolute_postmile as delta_postmile,
        a._valid_from,
        a._valid_to
    from station_meta as a
    inner join station_meta as b
        on
            a.freeway = b.freeway and a.direction = b.direction and a.station_type = b.station_type
            and a._valid_from = b._valid_from
    -- Most performance metrics are restricted to mainline and HV lanes.
    -- Furthermore, when looking at upstream and downstream stations, it
    -- does not make sense to include, e.g., ramps. So for the time being
    -- we restrict this table to HV and ML.
    where a.station_type in ('HV', 'ML')
),

nearest_downstream_station_pairs as (
    select
        station_id,
        other_station_id,
        district,
        freeway,
        direction,
        station_type,
        delta_postmile,
        _valid_from,
        _valid_to,
        row_number() over (partition by station_id, _valid_from order by abs(delta_postmile) asc)
            as distance_ranking
    from station_pairs
    where
        delta_postmile > 0
        and delta_postmile <= 5.0
        and station_id != other_station_id
),

nearest_upstream_station_pairs as (
    select
        station_id,
        other_station_id,
        district,
        freeway,
        direction,
        station_type,
        delta_postmile,
        _valid_from,
        _valid_to,
        row_number() over (partition by station_id, _valid_from order by abs(delta_postmile) asc)
            as distance_ranking
    from station_pairs
    where
        delta_postmile < 0
        and delta_postmile >= -5.0
        and station_id != other_station_id
),

self_pairs as (
    select
        *,
        0 as distance_ranking
    from station_pairs
    where station_id = other_station_id
),

nearest_station_pairs as (
    select
        station_id,
        other_station_id,
        district,
        freeway,
        direction,
        station_type,
        delta_postmile,
        _valid_from,
        _valid_to,
        distance_ranking
    from self_pairs
    union all
    select
        station_id,
        other_station_id,
        district,
        freeway,
        direction,
        station_type,
        delta_postmile,
        _valid_from,
        _valid_to,
        distance_ranking
    from nearest_downstream_station_pairs
    union all
    select
        station_id,
        other_station_id,
        district,
        freeway,
        direction,
        station_type,
        delta_postmile,
        _valid_from,
        _valid_to,
        distance_ranking
    from nearest_upstream_station_pairs
    order by district asc, freeway asc, station_id asc
),

-- assign the tag that is qulified for local and regional regression
nearest_station_pairs_with_tag as (
    select
        *,
        distance_ranking <= 1 as other_station_is_local
    from nearest_station_pairs
)

select * from nearest_station_pairs_with_tag

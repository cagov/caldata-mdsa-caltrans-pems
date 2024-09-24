{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref('int_vds__station_config') }}
),

mainline as (
    select
        station_id as ml_station_id,
        station_type,
        district,
        city,
        county,
        freeway,
        direction,
        name as ml_name,
        latitude as ml_lat,
        longitude as ml_long,
        absolute_postmile,
        physical_lanes as ml_lanes,
        _valid_from,
        _valid_to
    from station_meta
    where station_type in ('ML')
),

hov as (
    select
        station_id as hov_station_id,
        station_type,
        district,
        freeway,
        direction,
        name as hov_name,
        latitude as hov_lat,
        longitude as hov_long,
        absolute_postmile,
        physical_lanes as hov_lanes,
        _valid_from,
        _valid_to
    from station_meta
    where station_type in ('HV')
),

station_pairs as (
    select
        a.ml_station_id,
        b.hov_station_id,
        a.ml_name,
        a.ml_lanes,
        b.hov_name,
        b.hov_lanes,
        a.ml_lat,
        a.ml_long,
        b.hov_lat,
        b.hov_long,
        a.district,
        a.city,
        a.county,
        a.freeway,
        a.direction,
        abs(b.absolute_postmile - a.absolute_postmile) as delta_postmile,
        a._valid_from,
        a._valid_to
    from mainline as a
    inner join hov as b
        on
            a.freeway = b.freeway and a.direction = b.direction
            and a._valid_from = b._valid_from and a.ml_station_id != b.hov_station_id
),

closest_stations as (
    select
        *,
        row_number() over (partition by ml_station_id, _valid_from order by delta_postmile asc) as distance_ranking
    from station_pairs
)

select * from closest_stations
where distance_ranking = 1

with

station_meta as (
    select
        _valid_from,
        _valid_to,
        freeway,
        direction,
        type,
        absolute_postmile,
        id,
        latitude,
        longitude
    from {{ ref ("int_clearinghouse__station_meta") }}
    where type = 'ML' or type = 'HV'
),

calculate_distance_delta as (
    select
        *,
        absolute_postmile
        - lag(absolute_postmile)
            over (partition by _valid_from, freeway, direction, type order by absolute_postmile asc)
            as distance_delta
    from station_meta
)

select * from calculate_distance_delta

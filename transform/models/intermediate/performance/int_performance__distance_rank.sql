with

active_station as (
    select
        active_date,
        freeway,
        direction,
        type,
        to_decimal(absolute_postmile, 6, 3) as absolute_postmile,
        id
    from {{ ref ("int_clearinghouse__active_stations") }}
    where type = 'ML' or type = 'HV'
),

calculate_distance_rank as (
    select
        *,
        rank()
            over (partition by active_date, freeway, direction, type order by absolute_postmile asc)
            as distance_rank
    from active_station
    group by active_date, freeway, direction, type, absolute_postmile, id

),

calculate_distance_delta as (
    select
        *,
        absolute_postmile
        - lag(absolute_postmile)
            over (partition by active_date, freeway, direction, type order by distance_rank)
            as distance_delta_from_previous
    from calculate_distance_rank
)

select * from calculate_distance_delta

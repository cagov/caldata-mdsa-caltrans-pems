with

station_meta as (
    select
        meta_date,
        freeway,
        direction,
        type,
        lanes as lane,
        to_decimal(absolute_postmile, 6, 3) as absolute_postmile,
        id
    from {{ ref ("stg_clearinghouse__station_meta") }}
    where type = 'ML' or type = 'HV'
),

calculate_distance_rank as (
    select
        *,
        rank()
            over (partition by meta_date, freeway, direction, type, lane order by absolute_postmile asc)
            as distance_rank
    from station_meta
    group by meta_date, freeway, direction, type, lane, absolute_postmile, id

),

calculate_distance_delta as (
    select
        *,
        absolute_postmile
        - lag(absolute_postmile)
            over (partition by meta_date, freeway, direction, type, lane order by distance_rank)
            as distance_delta
    from calculate_distance_rank
)

select * from calculate_distance_delta

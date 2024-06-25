{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with

station_five_minute as (
    select
        id,
        sample_date,
        sample_timestamp,
        speed_weighted,
    from {{ ref ("int_clearinghouse__station_agg_five_minutes") }}
    where {{ make_model_incremental('sample_date') }}
),

station_meta as (
    select
        _valid_from,
        _valid_to,
        freeway,
        direction,
        type,
        absolute_postmile,
        id
    from {{ ref ("int_clearinghouse__station_meta") }}
    where type = 'ML' or type = 'HV'
),

five_minute_with_station_meta as (
    select
        sm.* exclude (id),
        f.* 
    from station_meta as sm
    inner join station_five_minute as f
        on
            sm._valid_from <= f.sample_date
            and sm._valid_to > f.sample_date
            and sm.id = f.id
),

calcs as (
    select
        *,
        lag(speed_weighted)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
             as speed_prev_ne, 

        lead(speed_weighted)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
            as speed_prev_sw,
        
        speed_weighted - lag(speed_weighted)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc) 
            as speed_delta_ne, 

        (lead(speed_weighted) - speed_weighted)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
            as speed_delta_sw,

        absolute_postmile
        - lag(absolute_postmile)
            over (partition by _valid_from, freeway, direction, type order by absolute_postmile asc)
            as distance_delta

    from five_minute_with_station_meta    
),

bottleneck_checks as (
    select
        *,
        case
            when
                speed_weighted < 40
                and distance_delta < 3
                and (speed_delta_ne <= -20 or speed_delta_sw <= -20)
                and (speed_prev_ne >= 40 or speed_prev_sw >= 40)
                then 1
            else 0
        end as bottleneck_start,
        case
            when
                speed_weighted < 40
                and distance_delta < 3
                and (speed_prev_ne < 40 or speed_prev_sw < 40)
                then 1
            else 0
        end as bottleneck_cont
    from calcs
    order by sample_timestamp, freeway, direction, type, absolute_postmile asc
)

select * from bottleneck_checks

-- bottleneck_cont_check as (
--     select
--         *,
--         sum(bottleneck_cont) over (
--             partition by sample_timestamp, freeway, direction, type order by sample_timestamp asc rows between current row and 6 following
--             ) as bottleneck_cont_sum
--     from unioned_data
-- )
        
-- select 
--     *,
--     iff(bottleneck_cont_sum >= 5, true, false) as is_bottleneck
--  from bottleneck_cont_check
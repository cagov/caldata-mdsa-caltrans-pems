{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "sample_date", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS")
) }}

with

station_five_minute as (
    select
        id,
        sample_date,
        sample_timestamp,
        speed_weighted
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
        id,
        latitude,
        longitude
    from {{ ref ("int_clearinghouse__station_meta") }}
    where type = 'ML' or type = 'HV'
),

five_minute_with_station_meta_and_detector_status as (
    select
        sm.* exclude (id, _valid_from, _valid_to),
        fm.*
    from station_five_minute as fm
    inner join station_meta as sm
        on
            fm.sample_date >= sm._valid_from
            and (fm.sample_date < sm._valid_to or sm._valid_to is null)
            and fm.id = sm.id
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

        lead(speed_weighted)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
        - speed_weighted
            as speed_delta_sw,

        absolute_postmile
        - lag(absolute_postmile)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
            as distance_delta_ne,

        lead(absolute_postmile)
            over (partition by sample_timestamp, freeway, direction, type order by absolute_postmile asc)
        - absolute_postmile
            as distance_delta_sw

    from five_minute_with_station_meta_and_detector_status
),

bottleneck_criteria as (
    select
        *,
        case
            when
                speed_weighted < 40
                and abs(distance_delta_ne) < 3
                and speed_delta_ne <= -20
                and (direction = 'N' or direction = 'E')
                then 1
            when
                speed_weighted < 40
                and abs(distance_delta_sw) < 3
                and speed_delta_sw <= -20
                and (direction = 'S' or direction = 'W')
                then 1
            else 0
        end as bottleneck_check

    from calcs
),

temporal_extent_check as (
    select
        *,
        sum(bottleneck_check) over (
            partition by id, sample_date
            order by sample_timestamp asc rows between current row and 6 following
        ) as count_of_bottleneck_check
    from bottleneck_criteria
),

temporal_extent as (
    select
        *,
        iff(count_of_bottleneck_check >= 5, true, false) as is_bottleneck
    from temporal_extent_check
)

select * from temporal_extent
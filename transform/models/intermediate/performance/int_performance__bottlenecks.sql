{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["station_id", "sample_date", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS")
) }}

with

station_five_minute as (
    select
        station_id,
        sample_date,
        sample_timestamp,
        nullifzero(speed_five_mins) as speed_five_mins,
        freeway,
        direction,
        station_type,
        absolute_postmile,
        length
    from {{ ref ("int_performance__station_metrics_agg_five_minutes") }}
    where
        {{ make_model_incremental('sample_date') }}
        and station_type in ('ML', 'HV')
),

calcs as (
    select
        *,

        /*Absolute postmile increases going north and east. When the direction of the freeway for a
        station is north or east, the "upstream" station has a smaller postmile, and we need to lag
        to get the speed there. When the direction is west or south, the "upstream" station has a
        larger postmile, and we need to lead to get the speed there. */

        speed_five_mins - lag(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
            as speed_delta_ne,

        speed_five_mins - lead(speed_five_mins)
            over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
            as speed_delta_sw,

        absolute_postmile - lag(absolute_postmile)
            over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
            as distance_delta_ne,

        absolute_postmile - lead(absolute_postmile)
            over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
            as distance_delta_sw

    from station_five_minute
),

bottleneck_criteria as (
    select
        *,
        case
            when
                speed_five_mins < 40
                and abs(distance_delta_ne) < 3
                and speed_delta_ne <= -20
                and (direction = 'N' or direction = 'E')
                then 1
            when
                speed_five_mins < 40
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
            partition by station_id, sample_date
            order by sample_timestamp asc rows between current row and 6 following
        ) as bottleneck_check_summed
    from bottleneck_criteria
),

temporal_extent as (
    select
        * exclude (bottleneck_check, bottleneck_check_summed),
        iff(bottleneck_check_summed >= 5, true, false) as is_bottleneck
    from temporal_extent_check
),

congestion as (
    select
        *,
        speed_five_mins < 40 as is_congested,

        /* Create a helper length field which is zero if we don't consider this station
        congested and the station length if we do. This will be summed later to get
        the congestion extent */
        iff(is_congested, length, 0) as congestion_length,

        /* Absolute postmile increases going north and east. When the direction of the freeway for a
        station is north or east, the "upstream" station has a smaller postmile, and we need to lag
        to get the speed there. When the direction is west or south, the "upstream" station has a
        larger postmile, and we need to lead to get the speed there. */
        case
            when direction in ('N', 'E')
                then
                    lag(is_congested)
                        over (
                            partition by sample_timestamp, freeway, direction, station_type
                            order by absolute_postmile asc
                        )
            when direction in ('S', 'W')
                then
                    lead(is_congested)
                        over (
                            partition by sample_timestamp, freeway, direction, station_type
                            order by absolute_postmile asc
                        )
        end as upstream_is_congested,
        iff(is_congested = upstream_is_congested, 0, 1) as congestion_status_change
    from temporal_extent
),

congestion_events as (
    select
        *,
        sum(congestion_status_change)
            over (
                partition by sample_timestamp, freeway, direction, station_type
                order by absolute_postmile asc
                rows between unbounded preceding and current row
            ) as congestion_sequence
    from congestion
),

congestion_length as (
    select
        *,
        case
            when direction in ('N', 'E')
                then
                    sum(congestion_length) over (
                        partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
                        order by absolute_postmile asc
                        rows between unbounded preceding and current row
                    )
            when direction in ('S', 'W')
                then
                    sum(congestion_length) over (
                        partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
                        order by absolute_postmile asc
                        rows between current row and unbounded following
                    )
        end as bottleneck_extent
    from congestion_events
    qualify is_bottleneck = true -- TODO: also filter if upstream is a bottleneck start?

)

select * from congestion_length

{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["station_id", "sample_date", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}

with

bottleneck_start as (
    select * from {{ ref ("int_performance__bottleneck_start") }}
    where {{ make_model_incremental('sample_date') }}
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
        iff(is_congested = upstream_is_congested, 0, 1) as congestion_status_change,
    from bottleneck_start
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
        sum(congestion_length) over (
            partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
            order by absolute_postmile asc
            rows between current row and unbounded following  -- This is a bug: should be preceding in some directions, following in others
        ) as bottleneck_extent
    from congestion_events
    qualify is_bottleneck = true  -- TODO: also filter if upstream is a bottleneck start?
)

select * from congestion_length
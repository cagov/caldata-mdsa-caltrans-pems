{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["station_id", "sample_date", "sample_timestamp"],
    on_schema_change='sync_all_columns',
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS")
) }}
{% set delay_metrics = ['delay_35_mph', 'delay_40_mph', 'delay_45_mph', 'delay_50_mph', 'delay_55_mph', 
'delay_60_mph'] %}

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
        length,
        volume_sum,
        delay_35_mph,
        delay_40_mph,
        delay_45_mph,
        delay_50_mph,
        delay_55_mph,
        delay_60_mph
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
        /*There are five routes (NS: Route 71; EW: Route 153, 282, 580, 780) in California which do not
        follow this rule. We need to specify them in the speed difference and distance difference
        calculation*/
        /*Need to check all calculations ordered by absolute_postmile and fix the logic for 5 routes
        specifically*/
        case
            when
                freeway in {{ var("special_routes") }}
                then speed_five_mins - lead(speed_five_mins)
                    over (
                        partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile desc
                    )
            else speed_five_mins - lead(speed_five_mins)
                over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
        end as speed_delta_ne,

        case
            when
                freeway in {{ var("special_routes") }}
                then absolute_postmile - lead(absolute_postmile)
                    over (
                        partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile desc
                    )
            else absolute_postmile - lead(absolute_postmile)
                over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
        end as distance_delta_ne,

        case
            when
                freeway in {{ var("special_routes") }}
                then speed_five_mins - lag(speed_five_mins)
                    over (
                        partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile desc
                    )
            else speed_five_mins - lag(speed_five_mins)
                over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
        end as speed_delta_sw,

        case
            when
                freeway in {{ var("special_routes") }}
                then absolute_postmile - lag(absolute_postmile)
                    over (
                        partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile desc
                    )
            else absolute_postmile - lag(absolute_postmile)
                over (partition by sample_timestamp, freeway, direction, station_type order by absolute_postmile asc)
        end as distance_delta_sw

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
        iff(bottleneck_check = 1 and bottleneck_check_summed >= 5, true, false) as is_bottleneck
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
        /*There are five routes (NS: Route 71; EW: Route 282, 580, 780) in California which do not
        follow this rule. We need to specify them in the speed difference and distance difference
        calculation*/
        case
            when (freeway in {{ var("special_routes") }} and direction in ('N', 'E'))
                then
                    lag(is_congested)
                        over (
                            partition by sample_timestamp, freeway, direction, station_type
                            order by absolute_postmile desc
                        )
            when (direction in ('N', 'E') and freeway not in {{ var("special_routes") }})
                then
                    lag(is_congested)
                        over (
                            partition by sample_timestamp, freeway, direction, station_type
                            order by absolute_postmile asc
                        )
            when (freeway in {{ var("special_routes") }} and direction in ('S', 'W'))
                then
                    lead(is_congested)
                        over (
                            partition by sample_timestamp, freeway, direction, station_type
                            order by absolute_postmile desc
                        )
            when (direction in ('S', 'W') and freeway not in {{ var("special_routes") }})
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
        case
            when freeway in {{ var("special_routes") }}
                then
                    sum(congestion_status_change)
                        over (
                            partition by sample_timestamp, freeway, direction, station_type
                            order by absolute_postmile desc
                            rows between unbounded preceding and current row
                        )
            else
                sum(congestion_status_change)
                    over (
                        partition by sample_timestamp, freeway, direction, station_type
                        order by absolute_postmile asc
                        rows between unbounded preceding and current row
                    )
        end as congestion_sequence
    from congestion
),

congestion_length as (
    select
        *,
        case
            when
                (direction in ('N', 'E') and freeway not in {{ var("special_routes") }})
                or (direction in ('S', 'W') and freeway in {{ var("special_routes") }})
                then
                    sum(congestion_length) over (
                        partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
                        order by absolute_postmile asc
                        rows between unbounded preceding and current row
                    )
            when
                (direction in ('S', 'W') and freeway not in {{ var("special_routes") }})
                or (direction in ('N', 'E') and freeway in {{ var("special_routes") }})
                then
                    sum(congestion_length) over (
                        partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
                        order by absolute_postmile asc
                        rows between current row and unbounded following
                    )
        end as bottleneck_extent
    from congestion_events
    qualify is_bottleneck = true -- TODO: also filter if upstream is a bottleneck start?

),

agg_spatial_delay as (
    select
        *,
        {% for delay in delay_metrics %}
            case
                when
                    (direction in ('N', 'E') and freeway not in {{ var("special_routes") }})
                    or (direction in ('S', 'W') and freeway in {{ var("special_routes") }})
                    then
                        sum({{ delay }}) over (
                            partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
                            order by absolute_postmile asc
                            rows between unbounded preceding and current row
                        )
                when
                    (direction in ('S', 'W') and freeway not in {{ var("special_routes") }})
                    or (direction in ('N', 'E') and freeway in {{ var("special_routes") }})
                    then
                        sum({{ delay }}) over (
                            partition by sample_timestamp, freeway, direction, station_type, congestion_sequence
                            order by absolute_postmile asc
                            rows between current row and unbounded following
                        )
            end as spatial_{{ delay }}
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}
    from congestion_length
),

shift as (
    select
        *,
        case
            when
                cast(sample_timestamp as time) >= {{ var('am_shift_start') }}
                and cast(sample_timestamp as time) <= {{ var('am_shift_end') }}
                then 'AM'
            when
                cast(sample_timestamp as time) >= {{ var('noon_shift_start') }}
                and cast(sample_timestamp as time) <= {{ var('noon_shift_end') }}
                then 'NOON'
            when
                cast(sample_timestamp as time) >= {{ var('pm_shift_start') }}
                and cast(sample_timestamp as time) <= {{ var('pm_shift_end') }}
                then 'PM'
        end as time_shift
    from agg_spatial_delay
),

bottleneck_delay as (
    select
        * exclude (
            congestion_sequence,
            congestion_status_change,
            is_bottleneck,
            speed_delta_ne,
            speed_delta_sw,
            distance_delta_sw,
            distance_delta_ne,
            volume_sum,
            length,
            upstream_is_congested,
            is_congested,
            speed_five_mins,
            congestion_length,
            delay_35_mph,
            delay_40_mph,
            delay_45_mph,
            delay_50_mph,
            delay_55_mph,
            delay_60_mph
        )
    from shift

)

select * from bottleneck_delay

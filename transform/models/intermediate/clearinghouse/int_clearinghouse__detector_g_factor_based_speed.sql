{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["detector_id", "sample_timestamp","sample_date"],
    on_schema_change="append_new_columns",
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}

with

detector_agg as (
    select
        detector_id,
        sample_date,
        sample_timestamp,
        station_id,
        lane,
        station_type,
        updated_volume_sum as volume_sum,
        updated_occupancy_avg as occupancy_avg,
        volume_observed
    from {{ ref('int_clearinghouse__detector_outlier_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

week_gen as (
    select
        *,
        date_trunc('week', sample_date) as week_start,
        date_trunc('hour', sample_timestamp) as hour,
        date_trunc('day', sample_date) as day
    from detector_agg
),

/* Generate 60-th percentile of the observed occupancies as occupancy threshold for a week dataset */
threshold as (
    select
        *,
        percentile_cont(0.6) within group (order by occupancy_avg)
            over (partition by detector_id, week_start)
            as occupancy_threshold,

        max(lane)
            over (partition by station_id, station_type)
            as lane_number

    from week_gen
),

/* Generate a table of free-flow speeds that are used to calculate g factor.
 * For detailed information, please refer to https://pems.dot.ca.gov/?dnode=Help&content=help_calc#speeds
*/
free_speed as (
    select
        *,
        case
            when
                station_type = 'HV'
                then 65
            when
                station_type = 'ML'
                and lane_number = 1
                then 65
            when
                station_type = 'ML'
                and lane_number = 2
                and lane = 1
                then 71.2
            when
                station_type = 'ML'
                and lane_number = 2
                and lane = 2
                then 65.1
            when
                station_type = 'ML'
                and lane_number = 3
                and lane = 1
                then 71.9
            when
                station_type = 'ML'
                and lane_number = 3
                and lane = 2
                then 69.7
            when
                station_type = 'ML'
                and lane_number = 3
                and lane = 3
                then 62.7
            when
                station_type = 'ML'
                and lane_number = 4
                and lane = 1
                then 74.8
            when
                station_type = 'ML'
                and lane_number = 4
                and lane = 2
                then 70.9
            when
                station_type = 'ML'
                and lane_number = 4
                and lane = 3
                then 67.4
            when
                station_type = 'ML'
                and lane_number = 4
                and lane = 4
                then 62.8
            when
                station_type = 'ML'
                and lane_number = 5
                and lane = 1
                then 76.5
            when
                station_type = 'ML'
                and lane_number = 5
                and lane = 2
                then 74.0
            when
                station_type = 'ML'
                and lane_number = 5
                and lane = 3
                then 72.0
            when
                station_type = 'ML'
                and lane_number = 5
                and lane = 4
                then 69.2
            when
                station_type = 'ML'
                and lane_number = 5
                and lane = 5
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 6
                and lane = 1
                then 76.5
            when
                station_type = 'ML'
                and lane_number = 6
                and lane = 2
                then 74.0
            when
                station_type = 'ML'
                and lane_number = 6
                and lane = 3
                then 72.0
            when
                station_type = 'ML'
                and lane_number = 6
                and lane = 4
                then 69.2
            when
                station_type = 'ML'
                and lane_number = 6
                and lane = 5
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 6
                and lane = 6
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 1
                then 76.5
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 2
                then 74.0
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 3
                then 72.0
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 4
                then 69.2
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 5
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 6
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 7
                and lane = 7
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 1
                then 76.5
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 2
                then 74.0
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 3
                then 72.0
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 4
                then 69.2
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 5
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 6
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 7
                then 64.5
            when
                station_type = 'ML'
                and lane_number = 8
                and lane = 8
                then 64.5
            else 64.5
        end as free_flow_speed

    from threshold
),

/* Calculate hourly based g factor, set as 22 if there is a null dataset in the mean function */
hourly_g_factor as (
    select
        *,
        coalesce(
            avg(case
                when occupancy_avg < occupancy_threshold
                    then occupancy_avg / nullifzero(volume_sum) * free_flow_speed * {{ var("mph_conversion") }}
            end)
                over (partition by detector_id, week_start, day, hour),
            {{ var("vehicle_effective_length") }})
            as raw_g_factor
    from free_speed
),

/* smoothing g factor */
smoothed_g_factor as (
    select
        *,
        avg(raw_g_factor) over (partition by detector_id, day order by hour rows between 6 preceding and 6 following)
            as g_factor
    from hourly_g_factor
),

/* Calculate exponential filter denoted as p factor */
p_factor_value as (
    select
        *,
        volume_sum / (volume_sum + {{ var("p_factor_smoothing_constant") }}) as p_factor
    from smoothed_g_factor
),

/* Calculate preliminary speed based on g factor, occupancy, and flow */
speed_preliminary_value as (
    select
        *,
        volume_sum * g_factor / nullifzero(occupancy_avg) * (1 / {{ var("mph_conversion") }}) as speed_preliminary
    from p_factor_value
),

speed_smoothed_value as (
    select
        spv.*,
        res.value_smoothed as speed_smoothed
    from
        speed_preliminary_value as spv,
        table(
            {{ target.database }}.public.exponential_smooth(spv.speed_preliminary, spv.p_factor::float)
                over (partition by spv.detector_id order by spv.sample_timestamp)
        ) as res
),

g_factor_speed_smoothed as (
    select
        *,
        case
            when
                lane = 1
                and speed_smoothed > 86.5
                then 86.5
            when
                lane = 2
                and speed_smoothed > 84
                then 84
            when
                lane = 3
                and speed_smoothed > 82
                then 82
            when
                lane = 4
                and speed_smoothed > 79.5
                then 79.5
            when
                lane in (5, 6, 7, 8)
                and speed_smoothed > 74.5
                then 74.5
            when
                lane in (1, 2, 3, 4, 5, 6, 7, 8)
                and speed_smoothed < 3
                then 3
            else speed_smoothed
        end as imputed_speed
    from speed_smoothed_value
)

select * from g_factor_speed_smoothed

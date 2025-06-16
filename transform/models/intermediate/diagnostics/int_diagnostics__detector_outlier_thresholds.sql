{{ config(
    materialized="incremental",
    unique_key=['detector_id', 'agg_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

-- Generate dates using dbt_utils.date_spine.
-- We choose choose an end_date so that at least outlier_agg_time_window
-- has passed so that we have as complete of data as possible.
with date_spine as (
    select date_day::date as agg_date
    from (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="'2023-01-01'",
            end_date=(
                "'"
                + (
                    modules.datetime.datetime.now()
                    - modules.datetime.timedelta(days=var("outlier_agg_time_window"))
                    - modules.datetime.timedelta(days=1)
                ).date().isoformat()
                + "'"
            )
        ) }}
    ) as spine
),

-- Filter dates to get the desired date sequence
agg_dates as (
    select *
    from date_spine
    where
        extract(day from agg_date) = 3
        and extract(month from agg_date) in (2, 5, 8, 11)
),

agg_dates_to_evaluate as (
    select * from agg_dates
    {% if is_incremental() %}
        minus
        select distinct agg_date from {{ this }}
    {% endif %}
),

-- Get all of the detectors that are producing good data, based on
-- the diagnostic tests
good_detectors as (
    select
        detector_id,
        district,
        sample_date
    from {{ ref("int_diagnostics__detector_status") }}
    where status = 'Good'
),

agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where station_type in ('ML', 'HV')
),

/* Get the raw five-minute data. This is joined on the
agg dates to only get samples which are within a week of
the agg date. It's also joined with the "good detectors"
table to only get samples from dates that we think were producing
good data. */
good_detector_data as (
    select
        agg.detector_id,
        agg.sample_date,
        agg.sample_timestamp,
        agg.volume_sum,
        agg.occupancy_avg,
        agg.speed_weighted,
        agg_dates_to_evaluate.agg_date
    from agg
    inner join agg_dates_to_evaluate
        on
            agg.sample_date::date >= agg_dates_to_evaluate.agg_date
            and agg.sample_date
            < dateadd(day, {{ var("outlier_agg_time_window") }}, agg_dates_to_evaluate.agg_date)
    where
        exists (
            select 1
            from good_detectors
            where
                good_detectors.detector_id = agg.detector_id
                and good_detectors.sample_date = agg.sample_date
        )
),

detector_outlier_thresholds as (
    select
        detector_id,
        agg_date,
        avg(volume_sum) as volume_mean,
        stddev(volume_sum) as volume_stddev,
        (percentile_cont(0.60) within group (order by volume_sum))::int as volume_60th,
        (percentile_cont(0.95) within group (order by volume_sum))::int as volume_95th,
        avg(occupancy_avg) as occupancy_mean,
        stddev(occupancy_avg) as occupancy_stddev,
        percentile_cont(0.60) within group (order by occupancy_avg) as occupancy_60th,
        percentile_cont(0.95) within group (order by occupancy_avg) as occupancy_95th,
        avg(speed_weighted) as speed_weighted_mean,
        stddev(speed_weighted) as speed_weighted_stddev,
        percentile_cont(0.60) within group (order by speed_weighted) as speed_weighted_60th,
        percentile_cont(0.95) within group (order by speed_weighted) as speed_weighted_95th
    from good_detector_data
    group by detector_id, agg_date
)

select * from detector_outlier_thresholds

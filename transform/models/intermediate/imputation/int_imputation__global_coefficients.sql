{{ config(
    materialized="incremental",
    unique_key=['station_id','lane', 'district', 'freeway', 'direction', 'station_type','regression_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(big="XL")
) }}

-- Generate dates using dbt_utils.date_spine
with date_spine as (
    select cast(date_day as date) as regression_date
    from (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="'1998-10-01'",
            end_date="current_date()"
        ) }}
    ) as spine
),

-- Filter dates to get the desired date sequence
regression_dates as (
    select *
    from date_spine
    where
        extract(day from regression_date) = 3
        and extract(month from regression_date) in (2, 5, 8, 11)
),

regression_dates_to_evaluate as (
    select * from regression_dates
    {% if is_incremental() %}
        minus
        select distinct regression_date from {{ this }}
    {% endif %}
),

-- Get all of the detectors that are producing good data, based on
-- the diagnostic tests
good_detectors as (
    select
        station_id,
        lane,
        district,
        sample_date
    from {{ ref("int_diagnostics__detector_status") }}
    where status = 'Good'
),

agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
),

/* Get the five-minute unimputed data. This is joined on the
regression dates to only get samples which are within a week of
the regression date. It's also joined with the "good detectors"
table to only get samples from dates that we think were producing
good data. */
detector_counts as (
    select
        agg.station_id,
        agg.lane,
        agg.sample_date,
        agg.sample_timestamp,
        agg.volume_sum,
        agg.occupancy_avg,
        agg.speed_weighted,
        agg.district,
        agg.freeway,
        agg.direction,
        agg.station_type,
        -- TODO: Can we give this a better name? Can we move this into the base model?
        coalesce(agg.speed_weighted, (agg.volume_sum * 22) / nullifzero(agg.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins,
        regression_dates_to_evaluate.regression_date
    from agg
    inner join regression_dates_to_evaluate
        on
            agg.sample_date >= regression_dates_to_evaluate.regression_date
            -- TODO: use variable for regression window
            and agg.sample_date
            < dateadd(day, {{ var("linear_regression_time_window") }}, regression_dates_to_evaluate.regression_date)
    inner join good_detectors
        on
            agg.station_id = good_detectors.station_id
            and agg.lane = good_detectors.lane
            and agg.sample_date = good_detectors.sample_date
    where agg.station_type in ('ML', 'HV') -- TODO: make a variable for "travel station types"
),

global_agg as (
    select
        sample_date,
        sample_timestamp,
        district,
        freeway,
        direction,
        station_type,
        /* Note: since this is an aggregate *across* stations rather than
        within a single station, it is more appropriate to average the sum
        rather than sum it. In any event, these averages are intended to be
        used for computing regression coefficients, so this just makes the
        regression coefficient the same up to a constant factor*/
        avg(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * speed_weighted) / nullifzero(sum(volume_sum)) as speed_weighted
    from detector_counts
    group by sample_date, sample_timestamp, district, freeway, direction, station_type
),

-- Join the 5-minute aggregated data with the district-freeway aggregation
detector_counts_with_global_averages as (
    select
        a.station_id,
        a.district,
        a.regression_date,
        a.lane,
        a.freeway,
        a.direction,
        a.station_type,
        a.speed_five_mins as speed,
        a.volume_sum as volume,
        a.occupancy_avg as occupancy,
        g.volume_sum as global_volume,
        g.occupancy_avg as global_occupancy,
        g.speed_weighted as global_speed
    from detector_counts as a
    inner join global_agg as g
        on
            a.sample_date = g.sample_date
            and a.sample_timestamp = g.sample_timestamp
            and a.district = g.district
            and a.freeway = g.freeway
            and a.direction = g.direction
            and a.station_type = g.station_type
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
detector_counts_regression as (
    select
        station_id,
        lane,
        district,
        freeway,
        station_type,
        direction,
        regression_date,
        -- speed regression model
        regr_slope(speed, global_speed) as speed_slope,
        regr_intercept(speed, global_speed) as speed_intercept,
        -- flow or volume regression model
        regr_slope(volume, global_volume) as volume_slope,
        regr_intercept(volume, global_volume) as volume_intercept,
        -- occupancy regression model
        regr_slope(occupancy, global_occupancy) as occupancy_slope,
        regr_intercept(occupancy, global_occupancy) as occupancy_intercept
    from detector_counts_with_global_averages
    group by station_id, lane, district, freeway, direction, station_type, regression_date
    -- No point in regressing if the variables are all null,
    -- this can save significant time.
    having
        (count(volume) > 0 and count(global_volume) > 0)
        or (count(occupancy) > 0 and count(global_occupancy) > 0)
        or (count(speed) > 0 and count(global_speed) > 0)
)

select * from detector_counts_regression

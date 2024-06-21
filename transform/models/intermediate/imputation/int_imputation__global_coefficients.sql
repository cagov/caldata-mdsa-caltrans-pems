{{ config(
        materialized="table",
        snowflake_warehouse=get_snowflake_warehouse(size="XL"),
    )
}}

/* This CTE is intended to be a placeholder for some
better-thought-out logic for what dates to evaluate regression
coefficients. As is, it is a hard-coded list of quarterly
dates starting in early 2023.
*/
with regression_dates as (
    select value::date as regression_date from table(
        flatten(
            [
                '2023-02-03'::date,
                '2023-05-03'::date,
                '2023-08-03'::date,
                '2023-11-03'::date,
                '2024-02-03'::date,
                '2024-05-03'::date
            ]
        )
    )
),

-- Get all of the detectors that are producing good data, based on
-- the diagnostic tests
good_detectors as (
    select
        station_id,
        lane,
        district,
        sample_date
    from {{ ref("int_diagnostics__real_detector_status") }}
    where status = 'Good'
),

/** TODO: thinking more and more we need to merge this at the five_minute_station_agg model. **/
station_meta as (
    select * from {{ ref("int_clearinghouse__station_meta") }}
    where type in ('ML', 'HV') -- TODO: do we want to do this?
),

global_agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes_district_freeway') }}
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
        agg.id,
        agg.lane,
        agg.sample_date,
        agg.sample_timestamp,
        agg.volume_sum,
        agg.occupancy_avg,
        agg.speed_weighted,
        agg.district,
        -- TODO: Can we give this a better name? Can we move this into the base model?
        coalesce(agg.speed_weighted, (agg.volume_sum * 22) / nullifzero(agg.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins,
        regression_dates.regression_date
    from agg
    inner join regression_dates
        on
            agg.sample_date >= regression_dates.regression_date
            -- TODO: use variable for regression window
            and agg.sample_date
            < dateadd(day, {{ var("linear_regression_time_window") }}, regression_dates.regression_date)
    inner join good_detectors
        on
            agg.id = good_detectors.station_id
            and agg.lane = good_detectors.lane
            and agg.sample_date = good_detectors.sample_date
),


detector_counts_with_meta as (
    select
        detector_counts.*,
        station_meta.freeway,
        station_meta.direction
    from detector_counts
    inner join station_meta
        on
            detector_counts.id = station_meta.id
            and detector_counts.sample_date >= station_meta._valid_from
            and (detector_counts.sample_date < station_meta._valid_to or station_meta._valid_to is null)
),

-- Join the 5-minute aggregated data with the district-freeway aggregation
detector_counts_with_global_averages as (
    select
        a.id,
        a.district,
        a.regression_date,
        a.lane,
        a.freeway,
        a.direction,
        a.speed_five_mins as speed,
        a.volume_sum as volume,
        a.occupancy_avg as occupancy,
        g.volume_sum as global_volume,
        g.occupancy_avg as global_occupancy,
        g.speed_weighted as global_speed -- TODO: what speed to use?
    from detector_counts_with_meta as a
    inner join global_agg as g
        on
            a.sample_date = g.sample_date
            and a.sample_timestamp = g.sample_timestamp
            and a.district = g.district
            and a.freeway = g.freeway
            and a.direction = g.direction
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
detector_counts_regression as (
    select
        id,
        lane,
        district,
        freeway,
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
    group by id, lane, district, freeway, direction, regression_date
    -- No point in regressing if the variables are all null,
    -- this can save significant time.
    having
        (count(volume) > 0 and count(global_volume) > 0)
        or (count(occupancy) > 0 and count(global_occupancy) > 0)
        or (count(speed) > 0 and count(global_speed) > 0)
)

select * from detector_counts_regression

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

-- Select all station pairs that are active for the chosen regression dates
nearby_stations as (
    select
        nearby.id,
        nearby.other_id
    from {{ ref('int_clearinghouse__nearby_stations') }} as nearby
    inner join regression_dates
        on
            nearby._valid_from <= regression_dates.regression_date
            and regression_dates.regression_date < coalesce(nearby._valid_to, current_date)
    -- only choose stations where they are actually reasonably close to each other,
    -- arbitrarily choose 1 mile
    where
        abs(nearby.delta_postmile) <= 1
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

agg as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
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
        good_detectors.district,
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

-- Self-join the 5-minute aggregated data with itself,
-- joining on the whether a station is itself or one
-- of it's neighbors. This is a big table, as we get
-- the product of all of the lanes in nearby stations
detector_counts_pairwise as (
    select
        a.id,
        b.id as other_id,
        a.district,
        a.regression_date,
        a.lane,
        b.lane as other_lane,
        a.speed_five_mins as speed,
        b.speed_five_mins as other_speed,
        a.volume_sum as volume,
        b.volume_sum as other_volume,
        a.occupancy_avg as occupancy,
        b.occupancy_avg as other_occupancy
    from detector_counts as a
    left join nearby_stations on a.id = nearby_stations.id
    inner join detector_counts as b
        on
            nearby_stations.other_id = b.id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
detector_counts_regression as (
    select
        id,
        other_id,
        lane,
        other_lane,
        district,
        regression_date,
        -- speed regression model
        regr_slope(speed, other_speed) as speed_slope,
        regr_intercept(speed, other_speed) as speed_intercept,
        -- flow or volume regression model
        regr_slope(volume, other_volume) as volume_slope,
        regr_intercept(volume, other_volume) as volume_intercept,
        -- occupancy regression model
        regr_slope(occupancy, other_occupancy) as occupancy_slope,
        regr_intercept(occupancy, other_occupancy) as occupancy_intercept
    from detector_counts_pairwise
    where not (id = other_id and lane = other_lane) -- don't bother regressing on self!
    group by id, other_id, lane, other_lane, district, regression_date
    -- No point in regressing if the variables are all null,
    -- this can save significant time.
    having
        (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
        or (count(speed) > 0 and count(other_speed) > 0)
)

select * from detector_counts_regression

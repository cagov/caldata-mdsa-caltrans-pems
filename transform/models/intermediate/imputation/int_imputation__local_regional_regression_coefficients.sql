{{ config(
    materialized="incremental",
    unique_key=['station_id','other_id','lane', 'other_lane','regression_date'],
    snowflake_warehouse=get_snowflake_warehouse(size="XL")
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

-- -- Filter dates to get the desired date sequence
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

-- Select all station pairs that are active for the chosen regression dates
nearby_stations as (
    select
        nearby.id,
        nearby.other_id,
        nearby.other_station_is_local
    from {{ ref('int_clearinghouse__nearby_stations') }} as nearby
    inner join regression_dates_to_evaluate
        on
            nearby._valid_from <= regression_dates_to_evaluate.regression_date
            and regression_dates_to_evaluate.regression_date < coalesce(nearby._valid_to, current_date)
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
    select *
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
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
        good_detectors.district,
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
),

-- Self-join the 5-minute aggregated data with itself,
-- joining on the whether a station is itself or one
-- of it's neighbors. This is a big table, as we get
-- the product of all of the lanes in nearby stations
detector_counts_pairwise as (
    select
        a.station_id,
        b.station_id as other_id,
        a.district,
        a.regression_date,
        a.lane,
        b.lane as other_lane,
        a.speed_five_mins as speed,
        b.speed_five_mins as other_speed,
        a.volume_sum as volume,
        b.volume_sum as other_volume,
        a.occupancy_avg as occupancy,
        b.occupancy_avg as other_occupancy,
        nearby_stations.other_station_is_local
    from detector_counts as a
    left join nearby_stations on a.station_id = nearby_stations.id
    inner join detector_counts as b
        on
            nearby_stations.other_id = b.station_id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
detector_counts_regression as (
    select
        station_id,
        other_id,
        lane,
        other_lane,
        district,
        regression_date,
        other_station_is_local,
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
    where not (station_id = other_id and lane = other_lane)-- don't bother regressing on self!
    group by station_id, other_id, lane, other_lane, district, regression_date, other_station_is_local
    -- No point in regressing if the variables are all null,
    -- this can save significant time.
    having
        (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
        or (count(speed) > 0 and count(other_speed) > 0)
)

select * from detector_counts_regression

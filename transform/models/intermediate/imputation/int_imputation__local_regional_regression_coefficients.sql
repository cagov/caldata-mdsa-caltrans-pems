{{ config(
    materialized="incremental",
    unique_key=['detector_id','other_detector_id','regression_date'],
    full_refresh=none,
    on_schema_change="append_new_columns",
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

-- Generate dates using dbt_utils.date_spine
-- We choose choose an end_date so that at least linear_regression_time_window
-- has passed so that we have as complete of data as possible.
with date_spine as (
    select cast(date_day as date) as regression_date
    from (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="'" + config.get("begin") + "'",
            end_date=(
                "'"
                + (
                    modules.datetime.datetime.now()
                    - modules.datetime.timedelta(days=var("linear_regression_time_window"))
                    - modules.datetime.timedelta(days=1)
                ).date().isoformat()
                + "'"
            )
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

agg as (
    select *
    from {{ ref('int_vds__detector_agg_five_minutes_normalized') }}
),

-- Select all station pairs that are active for the chosen regression dates
nearby_stations as (
    select
        nearby.station_id,
        nearby.other_station_id,
        nearby.other_station_is_local,
        regression_dates_to_evaluate.regression_date
    from {{ ref('int_vds__nearby_stations') }} as nearby
    inner join regression_dates_to_evaluate
        on
            {{ get_scd_2_data('regression_dates_to_evaluate.regression_date','nearby._valid_from','nearby._valid_to') }}

    /* This filters the nearby_stations model further to make sure we don't do the pairwise
    join below on more dates than we need. In theory, the parwise join *should* be able to
    do this filtering already, but in some profiling Snowflake was doing some join reordering
    that caused an unnecessary row explosion, where the date filtering was happening
    after the pairwise join. So this helps avoid that behavior. */
    where regression_dates_to_evaluate.regression_date >= (select min(sample_date) from agg)
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

/* Get the five-minute unimputed data. This is joined on the
regression dates to only get samples which are within a week of
the regression date. It's also joined with the "good detectors"
table to only get samples from dates that we think were producing
good data. */
detector_counts as (
    select
        agg.station_id,
        agg.detector_id,
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
            and agg.sample_date
            < dateadd(day, {{ var("linear_regression_time_window") }}, regression_dates_to_evaluate.regression_date)
    inner join good_detectors
        on
            agg.detector_id = good_detectors.detector_id
            and agg.sample_date = good_detectors.sample_date
),


-- Self-join the 5-minute aggregated data with itself,
-- joining on the whether a station is itself or one
-- of it's neighbors. This is a big table, as we get
-- the product of all of the lanes in nearby stations
detector_counts_pairwise as (
    select
        a.station_id,
        b.station_id as other_station_id,
        a.detector_id,
        b.detector_id as other_detector_id,
        a.district,
        a.regression_date,
        a.speed_five_mins as speed,
        b.speed_five_mins as other_speed,
        a.volume_sum as volume,
        b.volume_sum as other_volume,
        a.occupancy_avg as occupancy,
        b.occupancy_avg as other_occupancy,
        nearby_stations.other_station_is_local
    from detector_counts as a
    left join nearby_stations
        on
            a.station_id = nearby_stations.station_id
            and a.regression_date = nearby_stations.regression_date
    inner join detector_counts as b
        on
            nearby_stations.other_station_id = b.station_id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
detector_counts_regression as (
    select
        detector_id,
        other_detector_id,
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
    where not (detector_id = other_detector_id)-- don't bother regressing on self!
    group by detector_id, other_detector_id, district, regression_date, other_station_is_local
    -- No point in regressing if the variables are all null,
    -- this can save significant time.
    having
        (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
        or (count(speed) > 0 and count(other_speed) > 0)
)

select * from detector_counts_regression

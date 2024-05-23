{{ config(
    materialized="table",
    snowflake_warehouse="transforming_xl_dev",
) }}

with regression_dates as (
    select value::date as regression_date from table(
        flatten(
            [
                '2023-02-03'::date,
                '2023-05-03'::date,
                '2023-08-03'::date,
                '2023-11-03'::date
            ]
        )
    )
),

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

-- TODO: we should filter by the status of a station so that we are only trying to regress
-- between stations that are presumed operational
station_status as (
    select
        status.detector_id,
        status.station_id,
        status.district,
        len(status.lane_number) as lane,
        status._valid_from,
        status._valid_to
    from {{ ref('int_clearinghouse__station_status') }} as status
    inner join regression_dates
        on
            status._valid_from <= regression_dates.regression_date
            and regression_dates.regression_date < coalesce(status._valid_to, current_date)
),

station_counts as (
    select
        agg.id,
        agg.lane,
        agg.sample_date,
        agg.sample_timestamp,
        agg.volume_sum,
        agg.occupancy_avg,
        agg.speed_weighted,
        coalesce(agg.speed_weighted, (agg.volume_sum * 22) / nullifzero(agg.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins,
        regression_dates.regression_date
    from {{ ref('int_clearinghouse__five_minute_station_agg') }} as agg
    inner join regression_dates
        on
            agg.sample_date >= regression_dates.regression_date
            and agg.sample_date < dateadd(day, 7, regression_dates.regression_date)
),

-- Inner join on the station_status table to get rid of non-existent
-- lane numbers (e.g., the eighth lane on a two lane road)
station_counts_real_lanes as (
    select
        station_counts.*,
        station_status.district
    from station_counts
    inner join station_status
        on
            station_counts.id = station_status.station_id
            and station_counts.lane = station_status.lane
            and station_counts.sample_date >= station_status._valid_from
            and station_counts.sample_date < coalesce(station_status._valid_to, current_date)
),

-- Self-join the 5-minute aggregated data with itself,
-- joining on the whether a station is itself or one
-- of it's neighbors. This is a big table, as we get
-- the product of all of the lanes in nearby stations
station_counts_pairwise as (
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
    from station_counts_real_lanes as a
    left join nearby_stations on a.id = nearby_stations.id
    inner join station_counts_real_lanes as b
        on
            nearby_stations.other_id = b.id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
station_counts_regression as (
    select
        id,
        other_id,
        lane,
        other_lane,
        -- regression_date,
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
    from station_counts_pairwise
    where not (id = other_id and lane = other_lane)
    group by id, other_id, lane, other_lane, district, regression_date
    -- No point in regressing if the variables are all null,
    -- this saves a lot of time.
    having
        (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
        or (count(speed) > 0 and count(other_speed) > 0)
)

select * from station_counts_regression

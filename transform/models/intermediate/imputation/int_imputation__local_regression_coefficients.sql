{{ config(materialized="table") }}

with nearby_stations as (
    select
        id,
        other_id
    from {{ ref('int_clearinghouse__nearby_stations') }}
    -- only choose stations where they are actually reasonably close to each other,
    -- arbitrarily choose 1 mile
    where abs(delta_postmile) <= 1
),

-- TODO: we should filter by the status of a station so that we are only trying to regress
-- between stations that are presumed operational
station_status as (
    select
        id,
        district,
        active_date as sample_date,
        lanes as lane
    from {{ ref('int_clearinghouse__active_stations') }}
    where sample_date = dateadd(day, -3, current_date)
),

station_counts as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_weighted,
        coalesce(speed_weighted, (volume_sum * 22) / nullifzero(occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = dateadd(day, -3, current_date)
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
            station_counts.id = station_status.id
            and station_counts.lane = station_status.lane
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
        a.sample_date,
        a.sample_timestamp,
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
        district,
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
    group by id, other_id, lane, other_lane, district
    -- No point in regressing if the variables are all null,
    -- this saves a lot of time.
    having
        (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
        or (count(speed) > 0 and count(other_speed) > 0)
)

select * from station_counts_regression

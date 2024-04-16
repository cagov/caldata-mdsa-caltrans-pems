{{ config(materialized="table") }}

with nearby_stations as (
    select
        id,
        other_id
    from {{ ref('int_clearinghouse__nearby_stations') }}
    -- only choose stations where they are actually reasonably close to each other,
    -- arbitrarily choose 10 miles
    where delta_postmile < 10
),

-- TODO: we should filter by the status of a station so that we are only trying to regress
-- between stations that are presumed operational
station_status as (
    select 
        detector_id,
        station_id,
        district,
        len(lane_number) as lane
    from {{ ref('int_clearinghouse__most_recent_station_status')}}
),

station_counts as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = '2024-04-01'::date
),

-- Inner join on the station_status table to get rid of non-existent
-- lane numbers (e.g., the eighth lane on a two lane road)
station_counts_real_lanes as (
    select
        station_counts.*,
        station_status.district
    from station_counts
    inner join station_status
    on station_counts.id = station_status.station_id
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
        a.volume as volume,
        b.volume as other_volume,
        a.occupancy as occupancy,
        b.occupancy as other_occupancy
    from station_counts_real_lanes as a
    left join nearby_stations on a.id = nearby_stations.id
    inner join station_counts_real_lanes as b
    on nearby_stations.other_id = b.id
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
        regr_slope(volume, other_volume) as volume_slope,
        regr_intercept(volume, other_volume) as volume_intercept,
        regr_slope(occupancy, other_occupancy) as occupancy_slope,
        regr_intercept(occupancy, other_occupancy) as occupancy_intercept,
    from station_counts_pairwise
    group by id, other_id, lane, other_lane, district
    -- No point in regressing if the variables are all null,
    -- this saves a lot of time.
    having (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
)

select * from station_counts_regression

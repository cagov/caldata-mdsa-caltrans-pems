{{ config(materialized="table") }}

-- read the active stations only
with station_status as (
    select
        id,
        district,
        active_date as sample_date,
        lanes as lane
    from {{ ref('int_clearinghouse__active_stations') }}
    where sample_date = dateadd(day, -3, current_date)
),

-- read five mins agg table and calculate five mins speed using formula
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


-- now it seems that some of the station within the buffer have missing volume and occupancy
-- we will drop all the Null volumns and occupancy before developing multiple linear regression for the day
cleaned_null_data as (
    select *
    from station_counts_real_lanes
    where
        volume_sum is not null
        and occupancy_avg is not null
        and speed_five_mins is not null
),

-- read the stations that is within five mile buffer
nearby_stations as (select * from {{ ref('int_clearinghouse__global_nearby_stations') }}),

-- pair the performance metrics from nearest and distance detectors within five miles buffer
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
    from cleaned_null_data as a
    left join nearby_stations on a.id = nearby_stations.id
    left join cleaned_null_data as b
        on
            nearby_stations.other_id = b.id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- develop the simple linear regression model
station_counts_regression_model as (
    select
        id,
        district,
        sample_date,
        lane,
        other_lane,
        other_id,
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
    group by id, lane, other_lane, other_id, district, sample_date
)

select * from station_counts_regression_model

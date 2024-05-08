{{ config(materialized="table") }}

-- identify the station that is operational
with station_status as (
    select
        detector_id,
        station_id,
        district,
        len(lane_number) as lane
    from {{ ref('int_clearinghouse__most_recent_station_status') }}
    where detector_status = 'operational'
),

station_counts as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = '2024-05-05'::date
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
),

nearby_stations as (select * from {{ ref('int_cleaninghouse_global_nearby_station') }}),

-- pair the performance metrics from nearest and distance detectors within five miles buffer
station_counts_pairwise as (
    select
        a.id,
        b.id as other_id,
        a.district,
        a.sample_date,
        a.sample_timestamp,
        a.volume,
        b.volume as other_volume,
        a.occupancy,
        b.occupancy as other_occupancy
    from station_counts_real_lanes as a
    left join nearby_stations on a.id = nearby_stations.id
    left join station_counts_real_lanes as b
        on
            nearby_stations.other_id = b.id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- now it seems that some of the station within the buffer have missing volume and occupancy
-- we will drop all the Null volumns and occupancy before developing multiple linear regression for the day

cleaned_model_data as (
    select *
    from station_counts_pairwise
    where volume is not null and other_volume is not null and occupancy is not null and other_occupancy is not null
),

station_counts_regression_model as (
    select
        id,
        district,
        sample_date,
        lane,
        regr_slope(volume, other_volume) as volume_slope,
        regr_intercept(volume, other_volume) as volume_intercept,
        regr_slope(occupancy, other_occupancy) as occupancy_slope,
        regr_intercept(occupancy, other_occupancy) as occupancy_intercept
    from cleaned_model_data
    group by id, district, sample_date
)

select * from station_counts_regression_model

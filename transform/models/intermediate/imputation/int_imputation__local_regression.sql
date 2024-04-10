{{ config(materialized="table") }}

with nearby_stations as (
    select
        id,
        other_id
    from {{ ref('int_clearinghouse__nearby_stations') }}
    -- only choose stations where they are actually reasonably close to each other
    where delta_postmile < 20
),

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

station_counts_real_lanes as (
    select
        station_counts.*,
        station_status.district
    from station_counts
    inner join station_status
    on station_counts.id = station_status.station_id
        and station_counts.lane = station_status.lane
),

-- TODO: include same station
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
        avg(volume) as volume,
        avg(other_volume) as other_volume,
        count(volume) as cv,
        count(other_volume) as cov
    from station_counts_pairwise
    group by id, other_id, lane, other_lane, district
    having count(volume) > 0 and count(other_volume) > 0
)

select * from station_counts_regression


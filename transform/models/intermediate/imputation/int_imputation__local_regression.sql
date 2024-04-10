with nearby_stations as (
    select
        id,
        other_id
    from {{ ref('int_clearinghouse__nearby_stations') }}
    -- only choose stations where they are actually reasonably close to each other
    where delta_postmile < 20
),

station_counts as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = '2024-04-01'::date
),

-- TODO: include same station
station_counts_pairwise as (
    select
        a.id,
        b.id as other_id
        a.sample_date
        a.sample_timestamp,
        a.lane,
        b.lane as other_lane
        a.volume as volume,
        b.volume as other_volume,
        a.occupancy as occupancy,
        b.occupancy as other_occupancy
    from station_counts as a
    inner join station_counts as b
    on a.id = b.other_id
        and a.sample_date = b.sample_date
        and a.sample_timestamp = b.sample_timestamp
),

station_counts_regression as (
    select
        id,
        other_id,
        lane,
        other_lane,
        regr_slope(volume, other_volume) as slope,
        regr_intercept(volume, other_volume) as intercept
    from station_counts_pairwise
    group by id, other_id, lane, other_lane
)


{{ config(materialized='table') }}

with unimputed as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = '2024-04-01'::date
),

-- todo: this should be where status is not operational
missing as (
    select * from unimputed where volume is null or occupancy is null
),

coeffs as (
    select * from {{ ref('int_imputation__local_regression_coefficients') }}
),

missing_with_coeffs as (
    select
        missing.*,
        coeffs.other_id,
        coeffs.other_lane,
        coeffs.volume_slope,
        coeffs.volume_intercept,
        coeffs.occupancy_slope,
        coeffs.occupancy_intercept
    from missing
    left join coeffs
    on missing.id = coeffs.id
),

missing_with_neighbors as (
    select
        missing_with_coeffs.* exclude (volume, occupancy),
        unimputed.volume,
        unimputed.occupancy
    from missing_with_coeffs
    inner join unimputed
    on missing_with_coeffs.other_id = unimputed.id
        and missing_with_coeffs.other_lane = unimputed.lane
        and missing_with_coeffs.sample_date = unimputed.sample_date
        and missing_with_coeffs.sample_timestamp = unimputed.sample_timestamp
),

missing_imputed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        greatest(median(volume_slope * volume + volume_intercept), 0) as volume,
        least(greatest(median(occupancy_slope * occupancy + occupancy_intercept), 0), 1) as occupancy
    from missing_with_neighbors
    group by id, lane, sample_date, sample_timestamp
)

select * from missing_imputed
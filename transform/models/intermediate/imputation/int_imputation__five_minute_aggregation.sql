{{ config(materialized='table') }}
{% set sample_date = "'2024-04-01'::date" %}

with unimputed as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = {{ sample_date }}
),

status as (
    select * from {{ ref('int_pems__detector_status' )}}
    where status != 'Good'
        and sample_date = {{ sample_date }}
),

coeffs as (
    select * from {{ ref('int_imputation__local_regression_coefficients') }}
),

unimputed_with_status as (
    select
        unimputed.*,
        status.status
    from unimputed
    inner join status
    on unimputed.sample_date = status.sample_date
        and unimputed.id = status.station_id
        and unimputed.lane = status.lane
),

missing as (
    select * from unimputed_with_status
    where status != 'Good'
        or (volume is null and occupancy is null)
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
        and missing.lane = coeffs.lane
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
        least(greatest(median(occupancy_slope * occupancy + occupancy_intercept), 0), 1) as occupancy,
        true as imputed
    from missing_with_neighbors
    group by id, lane, sample_date, sample_timestamp
),

imputed as (
    select
        unimputed.*,
        missing_imputed.volume as imputed_volume,
        missing_imputed.occupancy as imputed_occupancy
    from unimputed
    left join missing_imputed
    on unimputed.sample_date = missing_imputed.sample_date
        and unimputed.id = missing_imputed.id
        and unimputed.lane = missing_imputed.lane
        and unimputed.sample_timestamp = missing_imputed.sample_timestamp
)

select * from imputed
{{ config(materialized='table') }}
{% set sample_date = "'2024-05-05'::date" %}

--  lets consider the unimputed dataframe

with unimputed as (
    select * from {{ ref('int_performance__five_min_perform_metrics') }}
    where sample_date = {{ sample_date }}
),

--  filter out the bad data that needs imputation
status as (
    select * from {{ ref('int_diagnostics__detector_status') }}
    where
        status != 'Good'
        and sample_date = {{ sample_date }}
),

-- read the global model
coeffs as (
    select * from {{ ref('int_imputation_global_regression_coefficients') }}
),

--  merge the unimputed dataframe with the dataframe that needs imputation
unimputed_with_status as (
    select
        unimputed.*,
        status.status
    from unimputed
    inner join status
        on
            unimputed.sample_date = status.sample_date
            and unimputed.id = status.station_id
            and unimputed.lane = status.lane
),

-- separate the dataframe that have only volume and occupancy missing
missing_vol_occ as (
    select * exclude (speed_five_mins)
    from unimputed_with_status
    where volume_sum is null and occupancy_avg is null
),

-- separate the dataframe that have only speed missing
missing_speed as (
    select * exclude (volume_sum, occupancy_avg)
    from unimputed_with_status
    where speed_five_mins is null
),

-- join the coeeficent with volume and occupancy missing dataframe
missing_vol_occ_with_coeffs as (
    select
        missing_vol_occ.*,
        coeffs.other_id,
        coeffs.other_lane,
        coeffs.volume_slope,
        coeffs.volume_intercept,
        coeffs.occupancy_slope,
        coeffs.occupancy_intercept
    from missing_vol_occ
    left join coeffs
        on
            missing_vol_occ.id = coeffs.id
            and missing_vol_occ.lane = coeffs.lane
),

-- join the coeeficent with speed missing dataframe
missing_speed_with_coeffs as (
    select
        missing_speed.*,
        coeffs.other_id,
        coeffs.other_lane,
        coeffs.speed_slope,
        coeffs.speed_intercept
    from missing_speed
    left join coeffs
        on
            missing_speed.id = coeffs.id
            and missing_speed.lane = coeffs.lane
),

--  read the neighbours that have volume and occupancy data
missing_vol_occ_with_neighbors as (
    select
        missing_vol_occ_with_coeffs.* exclude (volume_sum, occupancy_avg),
        unimputed.volume_sum,
        unimputed.occupancy_avg
    from missing_vol_occ_with_coeffs
    inner join unimputed
        on
            missing_vol_occ_with_coeffs.other_id = unimputed.id
            and missing_vol_occ_with_coeffs.other_lane = unimputed.lane
            and missing_vol_occ_with_coeffs.sample_date = unimputed.sample_date
            and missing_vol_occ_with_coeffs.sample_timestamp = unimputed.sample_timestamp
),

--  read the neighbours that have speed data
missing_speed_with_neighbors as (
    select
        missing_speed_with_coeffs.* exclude (speed_five_mins),
        unimputed.speed_five_mins
    from missing_speed_with_coeffs
    inner join unimputed
        on
            missing_speed_with_coeffs.other_id = unimputed.id
            and missing_speed_with_coeffs.other_lane = unimputed.lane
            and missing_speed_with_coeffs.sample_date = unimputed.sample_date
            and missing_speed_with_coeffs.sample_timestamp = unimputed.sample_timestamp
),

-- apply volume and occupancy imputation model
missing_imputed_vol_occ as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        greatest(median(volume_slope * volume_sum + volume_intercept), 0) as imputed_volume,
        least(greatest(median(occupancy_slope * occupancy_avg + occupancy_intercept), 0), 1) as imputed_occupancy,
        true as imputed
    from missing_vol_occ_with_neighbors
    group by id, lane, sample_date, sample_timestamp
),

-- apply speed imputation model
missing_imputed_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        least(greatest(median(speed_slope * speed_five_mins + speed_intercept), 0), 100) as imputed_speed_five_mins,
        true as imputed
    from missing_speed_with_neighbors
    group by id, lane, sample_date, sample_timestamp
),

-- Join speed prediction dataframe with predicted volume plus occupancy dataframe
imputed_vol_occ_speed_fused as (
    select
        missing_imputed_vol_occ.*,
        missing_imputed_speed.*
    from missing_imputed_vol_occ
    left join missing_imputed_speed
        on
            missing_imputed_vol_occ.sample_date = missing_imputed_speed.sample_date
            and missing_imputed_vol_occ.id = missing_imputed_speed.id
            and missing_imputed_vol_occ.lane = missing_imputed_speed.lane
            and missing_imputed_vol_occ.sample_timestamp = missing_imputed_speed.sample_timestamp
),

-- -- join imputed volume and occupancy with unimputed data
imputed_vol_occ_speed as (
    select
        unimputed.*,
        imputed_vol_occ_speed_fused.*
    from unimputed
    left join imputed_vol_occ_speed_fused
        on
            unimputed.sample_date = imputed_vol_occ_speed_fused.sample_date
            and unimputed.id = imputed_vol_occ_speed_fused.id
            and unimputed.lane = imputed_vol_occ_speed_fused.lane
            and unimputed.sample_timestamp = imputed_vol_occ_speed_fused.sample_timestamp
)

-- -- read the imputed outcomes
select * from imputed_vol_occ_speed

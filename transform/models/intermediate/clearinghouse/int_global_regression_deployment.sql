{{ config(materialized='table') }}

--  lets consider the unimputed dataframe

with unimputed as (
    select * from {{ ref('int_performance__five_min_perform_metrics') }}
    where sample_date = current_date - interval '4 day'
),

-- read the global model
coeffs as (
    select * from {{ ref('int_imputation_global_regression_coefficients') }}
),

-- separate the dataframe that have missing volume and occupancy
missing_vol_occ_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins
    from unimputed
    -- when volume and occupancy is present we already used the formula to calculate the speed
    -- here we will impute where all three has null value
    -- however, we will not impute where volime zero, occupancy zero and speed is null
    where volume_sum is null and occupancy_avg is null and speed_five_mins is null
),

-- join the coeeficent with missing volume and occupancy dataframe
missing_vol_occ_speed_with_coeffs as (
    select
        missing_vol_occ_speed.*,
        coeffs.other_id,
        coeffs.other_lane,
        coeffs.speed_slope,
        coeffs.speed_intercept,
        coeffs.volume_slope,
        coeffs.volume_intercept,
        coeffs.occupancy_slope,
        coeffs.occupancy_intercept
    from missing_vol_occ_speed
    left join coeffs
        on
            missing_vol_occ_speed.id = coeffs.id
            and missing_vol_occ_speed.lane = coeffs.lane
),

--  read the neighbours that have volume and occupancy data
missing_vol_occ_speed_with_neighbors as (
    select
        missing_vol_occ_speed_with_coeffs.* exclude (volume_sum, occupancy_avg, speed_five_mins),
        unimputed.speed_five_mins,
        unimputed.volume_sum,
        unimputed.occupancy_avg
    from missing_vol_occ_speed_with_coeffs
    inner join unimputed
        on
            missing_vol_occ_speed_with_coeffs.other_id = unimputed.id
            and missing_vol_occ_speed_with_coeffs.other_lane = unimputed.lane
            and missing_vol_occ_speed_with_coeffs.sample_date = unimputed.sample_date
            and missing_vol_occ_speed_with_coeffs.sample_timestamp = unimputed.sample_timestamp
),

-- apply volume and occupancy imputation model
missing_imputed_vol_occ_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        greatest(median(volume_slope * volume_sum + volume_intercept), 0) as volume_sum,
        least(greatest(median(occupancy_slope * occupancy_avg + occupancy_intercept), 0), 1) as occupancy_avg,
        least(greatest(median(speed_slope * speed_five_mins + speed_intercept), 0), 100) as speed_five_mins,
        true as is_imputed
    from missing_vol_occ_speed_with_neighbors
    group by id, lane, sample_date, sample_timestamp
),

-- separate the dataframe that have missing volume and occupancy
non_missing_vol_occ_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        false as is_imputed
    from unimputed
    where volume_sum is not null and occupancy_avg is not null
),

-- combine imputed and non-imputed volume and occupancy data frame together
global_regression_imputed_value as (
    select * from missing_imputed_vol_occ_speed
    union all
    select * from non_missing_vol_occ_speed
)

select * from global_regression_imputed_value

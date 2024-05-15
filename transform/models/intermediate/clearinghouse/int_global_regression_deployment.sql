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
missing_vol_occ as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg
    from unimputed
    where volume_sum is null and occupancy_avg is null
),

-- separate the dataframe that have only speed missing
missing_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        speed_five_mins
    from unimputed
    where speed_five_mins is null
),

-- join the coeeficent with missing volume and occupancy dataframe
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
            and missing_vol_occ.sample_date = coeffs.sample_date
),

-- join the coeeficent with missing speed dataframe
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
            and missing_speed.sample_date = coeffs.sample_date
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
        greatest(median(volume_slope * volume_sum + volume_intercept), 0) as volume_sum,
        least(greatest(median(occupancy_slope * occupancy_avg + occupancy_intercept), 0), 1) as occupancy_avg,
        true as imputed_vol_occ
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
        least(greatest(median(speed_slope * speed_five_mins + speed_intercept), 0), 100) as speed_five_mins,
        true as imputed_speed
    from missing_speed_with_neighbors
    group by id, lane, sample_date, sample_timestamp
),

-- separate the dataframe that have missing volume and occupancy
non_missing_vol_occ as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        false as imputed_vol_occ
    from unimputed
    where volume_sum is not null and occupancy_avg is not null
),

-- separate the dataframe that have only speed missing
non_missing_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        speed_five_mins,
        false as imputed_speed
    from unimputed
    where speed_five_mins is not null
),

--  combined imputed and non-imputed volume and occupancy dataframe together
imputed_non_imputed_speed as (
    select * from missing_imputed_speed
    union all
    select * from non_missing_speed
),

-- combine imputed and non-imputed volume and occupancy data frame together
imputed_non_imputed_vol_occ as (
    select * from missing_imputed_vol_occ
    union all
    select * from non_missing_vol_occ
),

-- now left join the 'speed' and 'vol and occupancy' dataframe together
global_regression_imputed_value as (
    select
        imputed_non_imputed_vol_occ.id,
        imputed_non_imputed_vol_occ.lane,
        imputed_non_imputed_vol_occ.sample_date,
        imputed_non_imputed_vol_occ.sample_timestamp,
        imputed_non_imputed_vol_occ.volume_sum,
        imputed_non_imputed_vol_occ.occupancy_avg,
        imputed_non_imputed_vol_occ.imputed_vol_occ,
        imputed_non_imputed_speed.speed_five_mins,
        imputed_non_imputed_speed.imputed_speed
    from imputed_non_imputed_vol_occ
    left join imputed_non_imputed_speed
        on
            imputed_non_imputed_vol_occ.id = imputed_non_imputed_speed.id
            and imputed_non_imputed_vol_occ.lane = imputed_non_imputed_speed.lane
            and imputed_non_imputed_vol_occ.sample_date = imputed_non_imputed_speed.sample_date
            and imputed_non_imputed_vol_occ.sample_timestamp = imputed_non_imputed_speed.sample_timestamp

)

select * from global_regression_imputed_value

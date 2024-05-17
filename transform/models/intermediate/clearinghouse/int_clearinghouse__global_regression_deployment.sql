{{ config(materialized='table') }}

--  lets consider the unimputed dataframe
with unimputed as (
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
    where sample_date = dateadd(day, -5, current_date)
),

counts_with_imputation_status as (
    select
        unimputed.*,
        case
            -- when volume and occupancy is present we already used the formula to calculate the speed
            -- here we will impute where all three has null value
            -- however, we will not impute where volime zero, occupancy zero and speed is null
            when
                unimputed.volume_sum is null and unimputed.occupancy_avg is null and unimputed.speed_five_mins is null
                then 'yes'
            else 'no'
        end as is_imputation_required
    from unimputed
),

-- separate the dataframe that have missing volume,occupancy and speed
missing_vol_occ_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins
    from counts_with_imputation_status
    where is_imputation_required = 'yes'
),

-- separate the dataframe that have already device recorded volume, speed and occ
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
    from counts_with_imputation_status
    where is_imputation_required = 'no'
),

-- read the global model
coeffs as (
    select * from {{ ref('int_clearinghouse__global_regression_coefficients') }}
),

-- join the coeeficent with missing volume,occupancy and speed dataframe
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

--  read the neighbours that have volume,occupancy and speed data from detectors
missing_vol_occ_speed_with_neighbors as (
    select
        missing_vol_occ_speed_with_coeffs.* exclude (volume_sum, occupancy_avg, speed_five_mins),
        non_missing_vol_occ_speed.speed_five_mins,
        non_missing_vol_occ_speed.volume_sum,
        non_missing_vol_occ_speed.occupancy_avg
    from missing_vol_occ_speed_with_coeffs
    inner join non_missing_vol_occ_speed
        on
            missing_vol_occ_speed_with_coeffs.other_id = non_missing_vol_occ_speed.id
            and missing_vol_occ_speed_with_coeffs.other_lane = non_missing_vol_occ_speed.lane
            and missing_vol_occ_speed_with_coeffs.sample_timestamp = non_missing_vol_occ_speed.sample_timestamp
),

-- apply imputation models to impute volume, occupancy and speed
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

-- combine imputed and non-imputed dataframe together
global_regression_imputed_value as (
    select * from missing_imputed_vol_occ_speed
    union all
    select * from non_missing_vol_occ_speed
)

-- select * from global_regression_imputed_value
select count(id) from global_regression_imputed_value

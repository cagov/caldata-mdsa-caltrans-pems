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
        extract(hour from sample_timestamp) as sample_hour,
        coalesce(speed_weighted, (volume_sum * 22) / nullifzero(occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = dateadd(day, -3, current_date)
),

-- classify the data that needs imputation
counts_with_imputation_status as (
    select
        unimputed.*,
        case
            -- when volume and occupancy is present we already used the formula to calculate the speed
            -- here we will impute where all three has null value
            -- where either volume or occupany only null along with null speed
            -- however, we will not impute where volime zero, occupancy zero and speed is null
            when
                (unimputed.volume_sum is null and unimputed.occupancy_avg is null and unimputed.speed_five_mins is null)
                or (
                    unimputed.volume_sum is not null
                    and unimputed.occupancy_avg is null
                    and unimputed.speed_five_mins is null
                )
                or (
                    unimputed.volume_sum is null
                    and unimputed.occupancy_avg is not null
                    and unimputed.speed_five_mins is null
                )
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
        -- sample_hour,
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
        -- sample_hour,
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
        coeffs.district,
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
),

-- join with the neighbours that have volume, occ and speed data
missing_vol_occ_speed_with_neighbors as (
    select
        missing_vol_occ_speed_with_coeffs.*,
        non_missing_vol_occ_speed.speed_five_mins as speed_five_mins_nbr,
        non_missing_vol_occ_speed.volume_sum as volume_sum_nbr,
        non_missing_vol_occ_speed.occupancy_avg as occupancy_avg_nbr
    from missing_vol_occ_speed_with_coeffs
    inner join non_missing_vol_occ_speed
        on
            missing_vol_occ_speed_with_coeffs.other_id = non_missing_vol_occ_speed.id
            and missing_vol_occ_speed_with_coeffs.sample_timestamp = non_missing_vol_occ_speed.sample_timestamp
),

-- apply imputation models to impute volume, occupancy and speed and flag id imputed
missing_imputed_vol_occ_speed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        -- Volume calculation
        coalesce(volume_sum, greatest(median(volume_slope * volume_sum_nbr + volume_intercept), 0))
            as volume_sum_imp,
        -- Flag for volume imputation
        coalesce(volume_sum is null, false) as is_imputed_volume,
        -- Occupancy calculation
        coalesce(
            occupancy_avg,
            least(greatest(median(occupancy_slope * occupancy_avg_nbr + occupancy_intercept), 0), 1)
        ) as occupancy_avg_imp,
        -- Flag for occupancy imputation
        coalesce(occupancy_avg is null, false) as is_imputed_occupancy,
        -- Speed calculation
        case
            when
                (speed_five_mins is null and volume_sum > 0 and occupancy_avg > 0)
                or (speed_five_mins is null and volume_sum is null and occupancy_avg is null)
                then least(greatest(median(speed_slope * speed_five_mins_nbr + speed_intercept), 0), 100)
            else speed_five_mins
        end as speed_five_mins_imp,
        -- Flag for speed imputation
        coalesce(speed_five_mins is null, false) as is_imputed_speed
    from
        missing_vol_occ_speed_with_neighbors
    group by id, lane, sample_date, sample_timestamp, volume_sum, occupancy_avg, speed_five_mins
),

-- due to having some model parameter nulls e.g. speed slope is null,
--  then you will get null prediction
-- We will flag all null prediction
missing_imputed_vol_occ_speed_flag as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum_imp,
        speed_five_mins_imp,
        occupancy_avg_imp,
        case
            when volume_sum_imp is not null then is_imputed_volume else false
        end as is_imputed_volume,
        case
            when occupancy_avg_imp is not null then is_imputed_occupancy else false
        end as is_imputed_occupancy,
        case
            when speed_five_mins_imp is not null then is_imputed_speed else false
        end as is_imputed_speed
    from missing_imputed_vol_occ_speed
),

-- -- combine imputed and non-imputed dataframe together
global_regression_imputed_value as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum_imp as volume_sum,
        occupancy_avg_imp as occupancy_avg,
        speed_five_mins_imp as speed_five_mins,
        is_imputed_volume,
        is_imputed_occupancy,
        is_imputed_speed
    from missing_imputed_vol_occ_speed_flag
    union all
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        false as is_imputed_volume,
        false as is_imputed_occupancy,
        false as is_imputed_speed
    from non_missing_vol_occ_speed
)

-- read the imputed and non-imputed dataframe
select * from global_regression_imputed_value

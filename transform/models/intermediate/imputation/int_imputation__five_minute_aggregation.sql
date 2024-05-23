{{ config(materialized='table') }}

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
            as speed_five_mins,
        (volume_sum is null) or (occupancy_avg is null) as imputation_required
    from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    where sample_date = dateadd(day, -3, current_date)
),

-- separate the dataframe that have missing volume,occupancy and speed
samples_requiring_imputation as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins
    from unimputed
    where imputation_required
),

-- separate the dataframe that have already device recorded volume, speed and occ
samples_not_requiring_imputation as (
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

-- read the model co-efficients
coeffs as (
    select * from {{ ref('int_imputation__local_regression_coefficients') }}
),

-- join the coeeficent with missing volume,occupancy and speed dataframe
samples_requiring_imputation_with_coeffs as (
    select
        samples_requiring_imputation.*,
        coeffs.other_id,
        coeffs.other_lane,
        coeffs.speed_slope,
        coeffs.speed_intercept,
        coeffs.volume_slope,
        coeffs.volume_intercept,
        coeffs.occupancy_slope,
        coeffs.occupancy_intercept,
        coeffs.regression_date
    from samples_requiring_imputation
    -- TODO: update sqlfluff to support asof joins
    asof join coeffs  -- noqa
        match_condition(samples_requiring_imputation.sample_date >= coeffs.regression_date)  -- noqa
        on samples_requiring_imputation.id = coeffs.id
),

--  read the neighbours that have volume, occupancy and speed data
samples_requiring_imputation_with_neighbors as (
    select
        imp.*,
        non_imp.speed_five_mins as speed_five_mins_nbr,
        non_imp.volume_sum as volume_sum_nbr,
        non_imp.occupancy_avg as occupancy_avg_nbr
    from samples_requiring_imputation_with_coeffs as imp
    inner join samples_not_requiring_imputation as non_imp
        on
            imp.other_id = non_imp.id
            and imp.sample_date = non_imp.sample_date
            and imp.sample_timestamp = non_imp.sample_timestamp
),

-- apply imputation models to impute volume, occupancy and speed and flag id imputed
imputed as (
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
        coalesce(speed_five_mins is null, false) as is_imputed_speed,
        any_value(regression_date) as regression_date
    from
        samples_requiring_imputation_with_neighbors
    group by id, lane, sample_date, sample_timestamp, volume_sum, occupancy_avg, speed_five_mins
),

-- due to having some model parameter nulls e.g. speed slope is null,
--  then you will get null prediction
-- We will flag all null prediction
imputed_flag as (
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
        end as is_imputed_speed,
        regression_date
    from imputed
),

-- combine imputed and non-imputed dataframe together
-- -- combine imputed and non-imputed dataframe together
local_regression_imputed_value as (
    select
        unimputed.*,
        imputed_flag.volume_sum_imp,
        imputed_flag.occupancy_avg_imp,
        imputed_flag.speed_five_mins_imp,
        imputed_flag.is_imputed_volume,
        imputed_flag.is_imputed_occupancy,
        imputed_flag.is_imputed_speed,
        imputed_flag.regression_date
    from unimputed
    left join imputed_flag
        on
            unimputed.id = imputed_flag.id
            and unimputed.lane = imputed_flag.lane
            and unimputed.sample_date = imputed_flag.sample_date
            and unimputed.sample_timestamp = imputed_flag.sample_timestamp
)

-- read the imputed and non-imputed dataframe
select * from local_regression_imputed_value

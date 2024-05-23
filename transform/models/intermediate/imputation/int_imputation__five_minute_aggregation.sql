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
    from unimputed
    where not imputation_required
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
        -- Occupancy calculation
        coalesce(
            occupancy_avg,
            least(greatest(median(occupancy_slope * occupancy_avg_nbr + occupancy_intercept), 0), 1)
        ) as occupancy_avg_imp,
        -- Speed calculation
        case
            when
                (speed_five_mins is null and volume_sum > 0 and occupancy_avg > 0)
                or (speed_five_mins is null and volume_sum is null and occupancy_avg is null)
                then least(greatest(median(speed_slope * speed_five_mins_nbr + speed_intercept), 0), 100)
            else speed_five_mins
        end as speed_five_mins_imp
    from
        samples_requiring_imputation_with_neighbors
    group by id, lane, sample_date, sample_timestamp, volume_sum, occupancy_avg, speed_five_mins
),

-- combine imputed and non-imputed dataframe together
-- -- combine imputed and non-imputed dataframe together
local_regression_imputed_value as (
    select
        unimputed.*,
        imputed.volume_sum_imp,
        imputed.occupancy_avg_imp,
        imputed.speed_five_mins_imp,
        imputed.is_imputed_volume,
        imputed.is_imputed_occupancy,
        imputed.is_imputed_speed,
        imputed.regression_date
    from unimputed
    left join imputed
        on
            unimputed.id = imputed.id
            and unimputed.lane = imputed.lane
            and unimputed.sample_date = imputed.sample_date
            and unimputed.sample_timestamp = imputed.sample_timestamp
)

-- read the imputed and non-imputed dataframe
select * from local_regression_imputed_value

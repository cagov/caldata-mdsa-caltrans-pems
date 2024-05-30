{{ config(materialized='table') }}

with unimputed as (
    select
        a.id,
        a.lane,
        a.sample_date,
        a.sample_timestamp,
        a.volume_sum,
        a.occupancy_avg,
        a.speed_weighted,
        (detectors.station_id is not null) as detector_is_good,
        coalesce(a.speed_weighted, (a.volume_sum * 22) / nullifzero(a.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from {{ ref('int_clearinghouse__five_minute_station_agg') }} as a
    left join {{ ref('int_diagnostics__good_detectors') }} as detectors
        on
            a.id = detectors.station_id
            and a.lane = detectors.lane
            and a.sample_date = detectors.sample_date
    where a.sample_date = '2024-04-01'
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
    where not detector_is_good
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
        speed_five_mins
    from unimputed
    where detector_is_good
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
        greatest(median(volume_slope * volume_sum_nbr + volume_intercept), 0) as volume,
        -- Occupancy calculation
        least(greatest(median(occupancy_slope * occupancy_avg_nbr + occupancy_intercept), 0), 1) as occupancy,
        -- Speed calculation
        greatest(median(speed_slope * speed_five_mins_nbr + speed_intercept), 0) as speed,
        any_value(regression_date) as regression_date
    from
        samples_requiring_imputation_with_neighbors
    group by id, lane, sample_date, sample_timestamp
),

-- combine imputed and non-imputed dataframe together
-- -- combine imputed and non-imputed dataframe together
agg_with_local_imputation as (
    select
        unimputed.*,
        imputed.volume as volume_local_regression,
        imputed.occupancy as occupancy_local_regression,
        imputed.speed as speed_local_regression,
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
select * from agg_with_local_imputation

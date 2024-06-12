{{ config(
        materialized='incremental',
        cluster_by=["sample_date"],
        unique_key=["id", "lane", "sample_timestamp"],
        snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL"),
    )
}}

-- Select unimputed data
with base as (
    select * from {{ ref('int_clearinghouse__five_minute_station_agg') }}
    {% if is_incremental() %}
    -- Look back two days to account for any late-arriving data
        where
            sample_date > (
                select
                    dateadd(
                        day,
                        {{ var("incremental_model_look_back") }},
                        max(sample_date)
                    )
                from {{ this }}
            )
            {% if target.name != 'prd' %}
                and sample_date
                >= dateadd(
                    day,
                    5 * {{ var("dev_model_look_back") }},
                    current_date()
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where sample_date >= dateadd(day, 5 * {{ var("dev_model_look_back") }}, current_date())
    {% endif %}
),

/* Join with the "good detectors"
model to flag whether we consider a detector to be operating
correctly for a given day.
*/
unimputed as (
    select
        base.id,
        base.lane,
        base.sample_date,
        base.sample_timestamp,
        base.volume_sum,
        base.occupancy_avg,
        base.speed_weighted,
        -- If the station_id in the join is not null, it means that the detector
        -- is considered to be "good" for a given date. TODO: likely restructure
        -- once the real_detectors model is eliminated.
        (detectors.station_id is not null) as detector_is_good,
        coalesce(base.speed_weighted, (base.volume_sum * 22) / nullifzero(base.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from base
    left join {{ ref('int_diagnostics__real_detector_status') }} as detectors
        on
            base.id = detectors.station_id
            and base.lane = detectors.lane
            and base.sample_date = detectors.sample_date
    where detectors.status = 'Good'
),

-- get the data that require imputation
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

-- get the data that does not require imputation
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

-- read the model coefficients
coeffs as (
    select * from {{ ref('int_imputation__local_regional_regression_coefficients') }}
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

-- Read the neighbours that have volume, occupancy and speed data.
-- We only join with neighbors that don't require imputation, as
-- there is no point to imputing bad data from bad data.
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

-- apply imputation models to impute volume, occupancy and speed
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

-- select the final CTE
select * from agg_with_local_imputation

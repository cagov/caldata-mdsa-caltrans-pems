{{ config(
        materialized='incremental',
        cluster_by=["sample_date"],
        unique_key=["id", "lane", "sample_timestamp"],
        snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL", small="XL"),
    )
}}

-- Select unimputed data
with base as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
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
                    {{ var("dev_model_look_back") }},
                    current_date()
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where sample_date >= dateadd(day, {{ var("dev_model_look_back") }}, current_date())
    {% endif %}
),

freeway_district_agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes_district_freeway') }}
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

/* Get all detectors that are "real" in that they represent lanes that exist
   (rather than lane 8 in a two lane road) with a status of "Good" */
good_detectors as (
    select * from {{ ref('int_diagnostics__real_detector_status') }}
    where status = 'Good'
),

/* Join with the "good detectors"
model to flag whether we consider a detector to be operating
correctly for a given day.
*/
unimputed as (
    select
        base.id,
        base.lane,
        base.district,
        base.sample_date,
        base.sample_timestamp,
        base.volume_sum,
        base.occupancy_avg,
        base.speed_weighted,
        -- If the station_id in the join is not null, it means that the detector
        -- is considered to be "good" for a given date. TODO: likely restructure
        -- once the real_detectors model is eliminated.
        (good_detectors.station_id is not null) as detector_is_good,
        coalesce(base.speed_weighted, (base.volume_sum * 22) / nullifzero(base.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from base
    left join good_detectors
        on
            base.id = good_detectors.station_id
            and base.lane = good_detectors.lane
            and base.sample_date = good_detectors.sample_date
),

-- get the data that require imputation
samples_requiring_imputation as (
    select
        id,
        lane,
        district,
        sample_date,
        sample_timestamp,
        volume_sum,
        occupancy_avg,
        speed_five_mins
    from unimputed
    where not detector_is_good
),

station_meta as (
    select * from {{ ref("int_clearinghouse__station_meta") }}
    where type in ('ML', 'HV') -- TODO: do we want to do this?
),

/** TODO: filter non-operational stations **/
samples_requiring_imputation_with_meta as (
    select
        samples_requiring_imputation.*,
        station_meta.freeway,
        station_meta.direction
    from samples_requiring_imputation
    inner join station_meta
        on
            samples_requiring_imputation.id = station_meta.id
            and samples_requiring_imputation.sample_date >= station_meta._valid_from
            and (samples_requiring_imputation.sample_date < station_meta._valid_to or station_meta._valid_to is null)
),

-- read the model coefficients
coeffs as (
    select * from {{ ref('int_imputation__global_coefficients') }}
),

-- join the coeeficent with missing volume,occupancy and speed dataframe
samples_requiring_imputation_with_coeffs as (
    select
        samples_requiring_imputation_with_meta.*,
        coeffs.speed_slope,
        coeffs.speed_intercept,
        coeffs.volume_slope,
        coeffs.volume_intercept,
        coeffs.occupancy_slope,
        coeffs.occupancy_intercept,
        coeffs.regression_date
    from samples_requiring_imputation_with_meta
    asof join coeffs
        match_condition (samples_requiring_imputation_with_meta.sample_date >= coeffs.regression_date)
        on
            samples_requiring_imputation_with_meta.id = coeffs.id
            and samples_requiring_imputation_with_meta.lane = coeffs.lane
            and samples_requiring_imputation_with_meta.district = coeffs.district
),

-- Read the neighbours that have volume, occupancy and speed data.
-- We only join with neighbors that don't require imputation, as
-- there is no point to imputing bad data from bad data.
samples_requiring_imputation_with_global as (
    select
        imp.*,
        non_imp.speed_weighted as speed_five_mins_global, -- TODO: what is the right speed?
        non_imp.volume_sum as volume_sum_global,
        non_imp.occupancy_avg as occupancy_avg_global
    from samples_requiring_imputation_with_coeffs as imp
    inner join freeway_district_agg as non_imp
        on
            imp.freeway = non_imp.freeway
            and imp.direction = non_imp.direction
            and imp.district = non_imp.district
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
        greatest(median(volume_slope * volume_sum_global + volume_intercept), 0) as volume,
        -- Occupancy calculation
        least(greatest(median(occupancy_slope * occupancy_avg_global + occupancy_intercept), 0), 1) as occupancy,
        -- Speed calculation
        greatest(median(speed_slope * speed_five_mins_global + speed_intercept), 0) as speed,
        any_value(regression_date) as regression_date
    from
        samples_requiring_imputation_with_global
    group by id, lane, sample_date, sample_timestamp
),

-- combine imputed and non-imputed dataframe together
agg_with_global_imputation as (
    select
        unimputed.*,
        imputed.volume as volume_global_regression,
        imputed.occupancy as occupancy_global_regression,
        imputed.speed as speed_global_regression,
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
select * from agg_with_global_imputation

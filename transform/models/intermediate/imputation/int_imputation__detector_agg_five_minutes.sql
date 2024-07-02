{{ config(
        materialized='incremental',
        cluster_by=["sample_date"],
        unique_key=["id", "lane", "sample_timestamp"],
        snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL", small="XS"),
    )
}}

-- Select unimputed data
with base as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

freeway_district_agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes_district_freeway') }}
    where {{ make_model_incremental('sample_date') }}
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

station_meta as (
    select * from {{ ref("int_clearinghouse__station_meta") }}
    where type in ('ML', 'HV') -- TODO: do we want to do this?
),

samples_requiring_imputation_with_meta as (
    select
        samples_requiring_imputation.*,
        station_meta.freeway,
        station_meta.direction,
        station_meta.type
    from samples_requiring_imputation
    inner join station_meta
        on
            samples_requiring_imputation.id = station_meta.id
            and samples_requiring_imputation.sample_date >= station_meta._valid_from
            and (samples_requiring_imputation.sample_date < station_meta._valid_to or station_meta._valid_to is null)
),

-- read the model coefficients
local_regional_coeffs as (
    select * from {{ ref('int_imputation__local_regional_regression_coefficients') }}
),

global_coeffs as (
    select * from {{ ref('int_imputation__global_coefficients') }}
),

-- join the coeeficent with missing volume,occupancy and speed dataframe for local Regression
samples_requiring_imputation_with_local_regional_coeffs as (
    select
        samples_requiring_imputation_with_meta.*,
        local_regional_coeffs.other_id,
        local_regional_coeffs.other_lane,
        local_regional_coeffs.speed_slope,
        local_regional_coeffs.speed_intercept,
        local_regional_coeffs.volume_slope,
        local_regional_coeffs.volume_intercept,
        local_regional_coeffs.occupancy_slope,
        local_regional_coeffs.occupancy_intercept,
        local_regional_coeffs.regression_date,
        local_regional_coeffs.other_station_is_local
    from samples_requiring_imputation_with_meta
    asof join local_regional_coeffs
        match_condition (samples_requiring_imputation_with_meta.sample_date >= local_regional_coeffs.regression_date)
        on samples_requiring_imputation_with_meta.id = local_regional_coeffs.id
),

-- join the coeeficent with missing volume,occupancy and speed dataframe
samples_requiring_imputation_with_global_coeffs as (
    select
        samples_requiring_imputation_with_meta.*,
        global_coeffs.speed_slope,
        global_coeffs.speed_intercept,
        global_coeffs.volume_slope,
        global_coeffs.volume_intercept,
        global_coeffs.occupancy_slope,
        global_coeffs.occupancy_intercept,
        global_coeffs.regression_date
    from samples_requiring_imputation_with_meta
    asof join global_coeffs
        match_condition (samples_requiring_imputation_with_meta.sample_date >= global_coeffs.regression_date)
        on
            samples_requiring_imputation_with_meta.id = global_coeffs.id
            and samples_requiring_imputation_with_meta.lane = global_coeffs.lane
            and samples_requiring_imputation_with_meta.district = global_coeffs.district
),

samples_requiring_imputation_with_global as (
    select
        imp.*,
        non_imp.speed_weighted as speed_five_mins_global, -- TODO: what is the right speed?
        non_imp.volume_sum as volume_sum_global,
        non_imp.occupancy_avg as occupancy_avg_global
    from samples_requiring_imputation_with_global_coeffs as imp
    inner join freeway_district_agg as non_imp
        on
            imp.freeway = non_imp.freeway
            and imp.direction = non_imp.direction
            and imp.type = non_imp.type
            and imp.district = non_imp.district
            and imp.sample_date = non_imp.sample_date
            and imp.sample_timestamp = non_imp.sample_timestamp
),

-- Read the local neighbours that have volume, occupancy and speed data.
-- We only join with neighbors that don't require imputation, as
-- there is no point to imputing bad data from bad data.
samples_requiring_imputation_with_local_neighbors as (
    select
        local_imp.*,
        non_imp.speed_five_mins as speed_five_mins_nbr,
        non_imp.volume_sum as volume_sum_nbr,
        non_imp.occupancy_avg as occupancy_avg_nbr
    from samples_requiring_imputation_with_local_regional_coeffs as local_imp
    inner join samples_not_requiring_imputation as non_imp
        on
            local_imp.other_id = non_imp.id
            and local_imp.sample_date = non_imp.sample_date
            and local_imp.sample_timestamp = non_imp.sample_timestamp
    where samples_requiring_imputation_with_local_regional_coeffs.other_station_is_local = true
),

-- Read the regional neighbours that have volume, occupancy and speed data.
-- We only join with neighbors that don't require imputation, as
-- there is no point to imputing bad data from bad data.
samples_requiring_imputation_with_regional_neighbors as (
    select
        regional_imp.*,
        non_imp.speed_five_mins as speed_five_mins_nbr,
        non_imp.volume_sum as volume_sum_nbr,
        non_imp.occupancy_avg as occupancy_avg_nbr
    from samples_requiring_imputation_with_local_regional_coeffs as regional_imp
    inner join samples_not_requiring_imputation as non_imp
        on
            regional_imp.other_id = non_imp.id
            and regional_imp.sample_date = non_imp.sample_date
            and regional_imp.sample_timestamp = non_imp.sample_timestamp
),

-- apply local imputation models to impute volume, occupancy and speed
local_imputed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        -- Volume calculation
        greatest(median(volume_slope * volume_sum_nbr + volume_intercept), 0) as volume_local_regression,
        -- Occupancy calculation
        least(greatest(median(occupancy_slope * occupancy_avg_nbr + occupancy_intercept), 0), 1)
            as occupancy_local_regression,
        -- Speed calculation
        greatest(median(speed_slope * speed_five_mins_nbr + speed_intercept), 0) as speed_local_regression,
        any_value(regression_date) as regression_date
    from
        samples_requiring_imputation_with_local_neighbors
    group by id, lane, sample_date, sample_timestamp
),

-- apply regional imputation models to impute volume, occupancy and speed
regional_imputed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        -- Volume calculation
        greatest(median(volume_slope * volume_sum_nbr + volume_intercept), 0) as volume_regional_regression,
        -- Occupancy calculation
        least(greatest(median(occupancy_slope * occupancy_avg_nbr + occupancy_intercept), 0), 1)
            as occupancy_regional_regression,
        -- Speed calculation
        greatest(median(speed_slope * speed_five_mins_nbr + speed_intercept), 0) as speed_regional_regression,
        any_value(regression_date) as regression_date
    from
        samples_requiring_imputation_with_regional_neighbors
    group by id, lane, sample_date, sample_timestamp
),

global_imputed as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        -- Volume calculation
        greatest(volume_slope * volume_sum_global + volume_intercept, 0) as volume_global_regression,
        -- Occupancy calculation
        least(greatest(occupancy_slope * occupancy_avg_global + occupancy_intercept, 0), 1)
            as occupancy_global_regression,
        -- Speed calculation
        greatest(speed_slope * speed_five_mins_global + speed_intercept, 0) as speed_global_regression,
        regression_date
    from
        samples_requiring_imputation_with_global
),

-- combine imputed and non-imputed dataframe together
agg_with_local_regional_global_imputation as (
    select
        unimputed.*,
        local_imputed.regression_date,
        local_imputed.volume_local_regression,
        local_imputed.occupancy_local_regression,
        local_imputed.speed_local_regression,
        regional_imputed.volume_regional_regression,
        regional_imputed.occupancy_regional_regression,
        regional_imputed.speed_regional_regression,
        global_imputed.volume_global_regression,
        global_imputed.occupancy_global_regression,
        global_imputed.speed_global_regression
    from unimputed
    left join local_imputed
        on
            unimputed.id = local_imputed.id
            and unimputed.lane = local_imputed.lane
            and unimputed.sample_date = local_imputed.sample_date
            and unimputed.sample_timestamp = local_imputed.sample_timestamp
    left join regional_imputed
        on
            unimputed.id = regional_imputed.id
            and unimputed.lane = regional_imputed.lane
            and unimputed.sample_date = regional_imputed.sample_date
            and unimputed.sample_timestamp = regional_imputed.sample_timestamp
    left join global_imputed
        on
            unimputed.id = global_imputed.id
            and unimputed.lane = global_imputed.lane
            and unimputed.sample_date = global_imputed.sample_date
            and unimputed.sample_timestamp = global_imputed.sample_timestamp
)

-- select the estimates from both local and regional regression
select * from agg_with_local_regional_global_imputation

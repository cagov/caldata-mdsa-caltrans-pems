{{ config(
        materialized='incremental',
        on_schema_change="append_new_columns",
        cluster_by=["sample_date"],
        unique_key=["station_id", "lane", "sample_timestamp", "sample_date"],
        snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL", small="XS"),
    )
}}

/* Unimputed data aggregated to five minutes" */
with base as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

/* Get all detectors that are "real" in that they represent lanes that exist
   (rather than lane 8 in a two lane road) with a status of "Good" */
good_detectors as (
    select * from {{ ref('int_diagnostics__real_detector_status') }}
    where status = 'Good'
),

/* Local/regional regression coefficients. These are pairwise betweens detectors
   that are near to each other. If they are within five miles, they are considered
   "regional". If they are in the same station or the immediate upstream/downstream
   station they are considered "local" */
local_regional_coeffs as (
    select * from {{ ref('int_imputation__local_regional_regression_coefficients') }}
),

/* Global regression coefficients. These are per-detector, and regress the detector's
   values with the freeway-direction-type-district average for those values at the same
   timestamp. */
global_coeffs as (
    select * from {{ ref('int_imputation__global_coefficients') }}
),

/* Join unimputed data with the "good detectors" model to flag whether we consider a
detector to be operating correctly for a given day. */
unimputed as (
    select
        base.station_id,
        base.detector_id,
        base.lane,
        base.district,
        base.sample_date,
        base.sample_timestamp,
        base.volume_sum,
        base.occupancy_avg,
        base.speed_weighted,
        base.freeway,
        base.direction,
        base.county,
        base.city,
        base.length,
        base.station_type,
        base.absolute_postmile,
        base.sample_ct,
        base.station_valid_from,
        base.station_valid_to,
        -- If the station_id in the join is not null, it means that the detector
        -- is considered to be "good" for a given date. TODO: likely restructure
        -- once the real_detectors model is eliminated.
        (good_detectors.station_id is not null) as detector_is_good,
        coalesce(base.speed_weighted, (base.volume_sum * 22) / nullifzero(base.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins
    from base
    left join good_detectors
        on
            base.station_id = good_detectors.station_id
            and base.lane = good_detectors.lane
            and base.sample_date = good_detectors.sample_date
    where base.station_type in ('ML', 'HV') -- TODO: make a variable for "travel station types"
),

/* Split the unimputed data into two sets, one requiring imputation
  (it's status is not "Good") and one not requiring imputation (it's status is
  "Good") */
samples_requiring_imputation as (
    select
        station_id,
        lane,
        district,
        sample_date,
        sample_timestamp,
        freeway,
        direction,
        station_type,
        volume_sum,
        occupancy_avg,
        speed_five_mins
    from unimputed
    where not detector_is_good
    -- there can still be gaps in detectors that are "Good",
    -- so we try to impute for those as well.
    or volume_sum is null
    or occupancy_avg is null
    or speed_five_mins is null
),

samples_not_requiring_imputation as (
    select
        station_id,
        lane,
        district,
        sample_date,
        sample_timestamp,
        freeway,
        direction,
        station_type,
        volume_sum,
        occupancy_avg,
        speed_five_mins
    from unimputed
    where detector_is_good
),

/** LOCAL/REGIONAL Regression follows **/

/* Join the samples requiring imputation with the local and regional
   coefficients. This will both give us the coefficients needed for
   regressing, as well as give us the ID/Lane of the other station
   that we'll be regressing against. This makes the number of rows
   increase significantly, as we get pairwise coefficients for a
   detector and all of its regional neighbors! */
samples_requiring_imputation_with_local_regional_coeffs as (
    select
        samples_requiring_imputation.*,
        local_regional_coeffs.other_station_id,
        local_regional_coeffs.other_lane,
        local_regional_coeffs.speed_slope,
        local_regional_coeffs.speed_intercept,
        local_regional_coeffs.volume_slope,
        local_regional_coeffs.volume_intercept,
        local_regional_coeffs.occupancy_slope,
        local_regional_coeffs.occupancy_intercept,
        local_regional_coeffs.regression_date,
        local_regional_coeffs.other_station_is_local
    from samples_requiring_imputation
    asof join local_regional_coeffs
        match_condition (samples_requiring_imputation.sample_date >= local_regional_coeffs.regression_date)
        on
            samples_requiring_imputation.station_id = local_regional_coeffs.station_id
            and samples_requiring_imputation.lane = local_regional_coeffs.lane
            and samples_requiring_imputation.district = local_regional_coeffs.district
),

/* Join with samples not requiring imputation based on the other ID/Lane
   of the regression coefficients. Here we filter for whether the
   regression coefficients are for a "local" station to get the subset
   of neighboring stations that are considered local. */
samples_requiring_imputation_with_local_neighbors as (
    select
        local_imp.*,
        non_imp.speed_five_mins as speed_five_mins_nbr,
        non_imp.volume_sum as volume_sum_nbr,
        non_imp.occupancy_avg as occupancy_avg_nbr
    from samples_requiring_imputation_with_local_regional_coeffs as local_imp
    inner join samples_not_requiring_imputation as non_imp
        on
            local_imp.other_station_id = non_imp.station_id
            and local_imp.sample_date = non_imp.sample_date
            and local_imp.sample_timestamp = non_imp.sample_timestamp
    where local_imp.other_station_is_local = true
),

/* Join with samples not requiring imputation based on the other ID/Lane
   of the regression coefficients. This is identical to the above, but does
   not filter for "local" stations, so these station pairs can be used
   to regress for all the regional neighbors. */
samples_requiring_imputation_with_regional_neighbors as (
    select
        regional_imp.*,
        non_imp.speed_five_mins as speed_five_mins_nbr,
        non_imp.volume_sum as volume_sum_nbr,
        non_imp.occupancy_avg as occupancy_avg_nbr
    from samples_requiring_imputation_with_local_regional_coeffs as regional_imp
    inner join samples_not_requiring_imputation as non_imp
        on
            regional_imp.other_station_id = non_imp.station_id
            and regional_imp.sample_date = non_imp.sample_date
            and regional_imp.sample_timestamp = non_imp.sample_timestamp
),

/* Actually do the local and regional imputation! We compute it for all
   the neighboring detectors, then aggregate up to the median of the imputed
   values, and finally clamp them to physical numbers (like greater than 0). */
local_imputed as (
    select
        station_id,
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
    group by station_id, lane, sample_date, sample_timestamp
),

regional_imputed as (
    select
        station_id,
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
    group by station_id, lane, sample_date, sample_timestamp
),

/** Global regression follows! **/

/* Join the samples requiring imputation with the global
   coefficients. */
samples_requiring_imputation_with_global_coeffs as (
    select
        samples_requiring_imputation.*,
        global_coeffs.speed_slope,
        global_coeffs.speed_intercept,
        global_coeffs.volume_slope,
        global_coeffs.volume_intercept,
        global_coeffs.occupancy_slope,
        global_coeffs.occupancy_intercept,
        global_coeffs.regression_date
    from samples_requiring_imputation
    asof join global_coeffs
        match_condition (samples_requiring_imputation.sample_date >= global_coeffs.regression_date)
        on
            samples_requiring_imputation.station_id = global_coeffs.station_id
            and samples_requiring_imputation.lane = global_coeffs.lane
            and samples_requiring_imputation.district = global_coeffs.district
),

/* Aggregate the samples not requiring imputation up to the freeway/district/station-type
   level. This creates the value against which we will be aggregating for each timestamp.
   It's important that this aggregation look idential to that in
   int_imputation__global_coefficients, otherwise the regression will be wrong. */
freeway_district_agg as (
    select
        sample_date,
        sample_timestamp,
        district,
        freeway,
        direction,
        station_type,
        /* Note: since this is an aggregate *across* stations rather than
        within a single station, it is more appropriate to average the sum
        rather than sum it. In any event, these averages are intended to be
        used for computing regression coefficients, so this just makes the
        regression coefficient the same up to a constant factor*/
        avg(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as speed_five_mins
    from samples_not_requiring_imputation
    group by sample_date, sample_timestamp, district, freeway, direction, station_type
),

/* Join the averages in with the samples requiring imputation. */
samples_requiring_imputation_with_global as (
    select
        imp.*,
        non_imp.speed_five_mins as speed_five_mins_global,
        non_imp.volume_sum as volume_sum_global,
        non_imp.occupancy_avg as occupancy_avg_global
    from samples_requiring_imputation_with_global_coeffs as imp
    inner join freeway_district_agg as non_imp
        on
            imp.freeway = non_imp.freeway
            and imp.direction = non_imp.direction
            and imp.station_type = non_imp.station_type
            and imp.district = non_imp.district
            and imp.sample_date = non_imp.sample_date
            and imp.sample_timestamp = non_imp.sample_timestamp
),

/* Finally, do the global imputation! */
global_imputed as (
    select
        station_id,
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

/** Put the local, regional, and global datasets all together **/
agg_with_local_regional_global_imputation as (
    select
        unimputed.*,
        local_imputed.regression_date as local_regression_date,
        local_imputed.volume_local_regression,
        local_imputed.occupancy_local_regression,
        local_imputed.speed_local_regression,
        regional_imputed.regression_date as regional_regression_date,
        regional_imputed.volume_regional_regression,
        regional_imputed.occupancy_regional_regression,
        regional_imputed.speed_regional_regression,
        global_imputed.regression_date as global_regression_date,
        global_imputed.volume_global_regression,
        global_imputed.occupancy_global_regression,
        global_imputed.speed_global_regression
    from unimputed
    left join local_imputed
        on
            unimputed.station_id = local_imputed.station_id
            and unimputed.lane = local_imputed.lane
            and unimputed.sample_date = local_imputed.sample_date
            and unimputed.sample_timestamp = local_imputed.sample_timestamp
    left join regional_imputed
        on
            unimputed.station_id = regional_imputed.station_id
            and unimputed.lane = regional_imputed.lane
            and unimputed.sample_date = regional_imputed.sample_date
            and unimputed.sample_timestamp = regional_imputed.sample_timestamp
    left join global_imputed
        on
            unimputed.station_id = global_imputed.station_id
            and unimputed.lane = global_imputed.lane
            and unimputed.sample_date = global_imputed.sample_date
            and unimputed.sample_timestamp = global_imputed.sample_timestamp
)

select * from agg_with_local_regional_global_imputation

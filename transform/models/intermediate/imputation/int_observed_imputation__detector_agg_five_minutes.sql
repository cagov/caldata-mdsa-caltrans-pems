{{ config(
    materialized="incremental",
    unique_key=['id','lane','direction','sample_date', 'sample_timestamp'],
    snowflake_warehouse=get_snowflake_warehouse(size="XL")
) }}

-- read the observed five minutes data
with
observed_five_minutes_agg as (
    select *
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

-- read imputed five minutes data
imputed_five_minutes_agg as (
    select
        id,
        lane,
        sample_date,
        sample_timestamp,
        local_regression_date,
        regional_regression_date,
        global_regression_date,
        volume_local_regression,
        volume_regional_regression,
        volume_global_regression,
        speed_local_regression,
        speed_regional_regression,
        speed_global_regression,
        occupancy_local_regression,
        occupancy_regional_regression,
        occupancy_global_regression
    from {{ ref('int_imputation__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

-- now join observed and imputed five mins agg
obserbed_and_imputed_five_mins_agg as (
    select
        observed_five_minutes_agg.*,
        imputed_five_minutes_agg.* exclude (id, lane, sample_date, sample_timestamp)
    from observed_five_minutes_agg
    left join imputed_five_minutes_agg
        on
            observed_five_minutes_agg.id = imputed_five_minutes_agg.id
            and observed_five_minutes_agg.lane = imputed_five_minutes_agg.lane
            and observed_five_minutes_agg.sample_date = imputed_five_minutes_agg.sample_date
            and observed_five_minutes_agg.sample_timestamp = imputed_five_minutes_agg.sample_timestamp
),

-- now select the final speed, volume and occupancy
-- tag if it is observed or imputed
hybrid_five_mins_agg as (
    select
        obserbed_and_imputed_five_mins_agg.* exclude (
            volume_sum, speed_weighted, occupancy_avg,
            local_regression_date,
            regional_regression_date,
            global_regression_date,
            volume_local_regression,
            volume_regional_regression,
            volume_global_regression,
            speed_local_regression,
            speed_regional_regression,
            speed_global_regression,
            occupancy_local_regression,
            occupancy_regional_regression,
            occupancy_global_regression
        ),
        -- select a single volume_sum based on obs or imputed
        coalesce(volume_sum, volume_local_regression, volume_regional_regression, volume_global_regression)
            as volume_sum,
        not coalesce(volume_sum is not null, false) as volume_sum_imputed,
        -- select a single speed_weighted based on obs or imputed
        coalesce(speed_weighted, speed_local_regression, speed_regional_regression, speed_global_regression)
            as speed_weighted,
        not coalesce(speed_weighted is not null, false) as speed_weighted_imputed,
        -- select a single occupancy average based on obs or imputed
        coalesce(
            occupancy_avg, occupancy_local_regression, occupancy_regional_regression, occupancy_global_regression
        ) as occupancy_avg,
        not coalesce(occupancy_avg is not null, false) as occupancy_avg_replaced
    from obserbed_and_imputed_five_mins_agg
)

-- select the final observed and imputed five mins agg table
select * from hybrid_five_mins_agg

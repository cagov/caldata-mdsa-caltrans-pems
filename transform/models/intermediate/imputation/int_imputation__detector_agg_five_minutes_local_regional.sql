{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane","sample_date", "sample_timestamp",'regression_date'],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

-- read local region estimates
with local_reg as (
    select
        id,
        lane,
        speed_local_regression,
        occupancy_local_regression,
        volume_local_regression,
        detector_is_good,
        regression_date,
        sample_date,
        sample_timestamp
    from {{ ref('int_imputation__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

-- read regional regression estimates
regional_reg as (
    select
        id,
        lane,
        detector_is_good,
        regression_date,
        sample_date,
        sample_timestamp,
        imp_occupancy as occupancy_regional_regression,
        imp_volume as volume_regional_regression,
        imp_speed as speed_regional_regression
    from {{ ref('int_imputation__detector_agg_five_minutes_regional_reg') }}
    where {{ make_model_incremental('sample_date') }}
),

--join regional regression with local regression
local_regional_estimates as (
    select
        local_reg.*,
        regional_reg.occupancy_regional_regression,
        regional_reg.volume_regional_regression,
        regional_reg.speed_regional_regression
    from local_reg
    left join regional_reg
        on
            local_reg.id = regional_reg.id
            and local_reg.lane = regional_reg.lane
            and local_reg.sample_timestamp = regional_reg.sample_timestamp
            and local_reg.regression_date = regional_reg.regression_date
)

select * from local_regional_estimates

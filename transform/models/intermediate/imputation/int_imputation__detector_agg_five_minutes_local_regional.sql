{{ config(materialized='table') }}

--join regional regression with local regression
with local_regional_para as (
    select
        lrp.*,
        rrp.imp_occupancy as occupancy_regional_regression,
        rrp.imp_volume as volume_regional_regression,
        rrp.imp_speed as speed_regional_regression
    from {{ ref('int_imputation__detector_agg_five_minutes') }} as lrp
    left join {{ ref('int_imputation__detector_agg_five_minutes_regional_reg') }} as rrp
        on
            lrp.id = rrp.id
            and lrp.lane = rrp.lane
            and lrp.sample_timestamp = rrp.sample_timestamp
            and lrp.regression_date = rrp.regression_date
)

select * from local_regional_para

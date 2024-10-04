{{ config(
    materialized="table"
) }}

with
detector_status_daily_count as (
    /*
    * This CTE returns the number of total rows created daily in
    * the int_diagnostics__detector_status model. This count should be
    * to checked against the daily detector counts for the following models:
    * - int_clearinghouse__detector_agg_five_minutes_with_missing_rows
    * - int_imputation__detector_imputed_agg_five_minutes
    * - int_performance__detector_metrics_agg_five_minutes
    * The daily detector counts for these tables should match
    */
    select
        sample_date,
        count_if(status = 'Good') as good_status_count,
        count_if(status != 'Good') as bad_status_count,
        count(*) as total_count
    from {{ ref('int_diagnostics__detector_status') }}
    group by sample_date
),

-- Clearinghouse Detector Count per Day
clearinghouse_detector_daily_count as (
    select
        sample_date,
        count(distinct detector_id) as clearinghouse_detector_count
    from {{ ref('int_clearinghouse__detector_agg_five_minutes_with_missing_rows') }}
    group by sample_date
),

-- Imputation Detector Count per Day
imputation_detector_daily_count as (
    select
        sample_date,
        count(distinct detector_id) as imputation_detector_count
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    group by sample_date
),

-- Performance Detector Count per Day
performance_detector_daily_count as (
    select
        sample_date,
        count(distinct detector_id) as performance_detector_count
    from {{ ref('int_performance__detector_metrics_agg_five_minutes') }}
    group by sample_date
),

daily_detector_count_check as (
    select
        dsdc.*,
        cddc.clearinghouse_detector_count,
        iddc.imputation_detector_count,
        pddc.performance_detector_count
    from detector_status_daily_count as dsdc
    left join clearinghouse_detector_daily_count as cddc
        on dsdc.sample_date = cddc.sample_date
    left join imputation_detector_daily_count as iddc
        on dsdc.sample_date = iddc.sample_date
    left join performance_detector_daily_count as pddc
        on dsdc.sample_date = pddc.sample_date
)

select * from daily_detector_count_check

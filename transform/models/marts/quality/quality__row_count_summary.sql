with

ML_HV_DETECTOR_STATUS_DAILY_COUNT as (
    /*
    * This CTE returns the number of total rows created daily in
    * the int_diagnostics__detector_status model. This count should be
    * to checked against the daily detector counts for the following models:
    * - int_clearinghouse__detector_agg_five_minutes_with_missing_rows
    * - int_imputation__detector_imputed_agg_five_minutes
    * - int_performance__detector_metrics_agg_five_minutes
    * The daily detector counts for these models should match for
    */
    select
        SAMPLE_DATE,
        count(*) as ML_HV_DETECTOR_STATUS_TOTAL_COUNT
    from {{ ref('int_diagnostics__detector_status') }}
    where STATION_TYPE = 'ML' or STATION_TYPE = 'HV'
    group by SAMPLE_DATE
),

-- Clearinghouse Detector Count per Day
ML_HV_CLEARINGHOUSE_DETECTOR_DAILY_COUNT as (
    select
        SAMPLE_DATE,
        count(distinct DETECTOR_ID) as ML_HV_CLEARINGHOUSE_DETECTOR_COUNT
    from {{ ref('int_clearinghouse__detector_agg_five_minutes_with_missing_rows') }}
    where STATION_TYPE = 'ML' or STATION_TYPE = 'HV'
    group by SAMPLE_DATE
),

-- Imputation Detector Count per Day
ML_HV_IMPUTATION_DETECTOR_DAILY_COUNT as (
    select
        SAMPLE_DATE,
        count(distinct DETECTOR_ID) as ML_HV_IMPUTATION_DETECTOR_COUNT
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    group by SAMPLE_DATE
),

-- Performance Detector Count per Day
ML_HV_PERFORMANCE_DETECTOR_DAILY_COUNT as (
    select
        SAMPLE_DATE,
        count(distinct DETECTOR_ID) as ML_HV_PERFORMANCE_DETECTOR_COUNT
    from {{ ref('int_performance__detector_metrics_agg_five_minutes') }}
    group by SAMPLE_DATE
),

-- Returns count for all station types per Day
DETECTOR_STATUS_COUNT as (
    select
        SAMPLE_DATE,
        count_if(STATUS = 'Good') as GOOD_STATUS_COUNT,
        count_if(STATUS != 'Good') as BAD_STATUS_COUNT,
        count(*) as ALL_DETECTOR_STATUS_TOTAL_COUNT
    from {{ ref('int_diagnostics__detector_status') }}
    group by SAMPLE_DATE
),

DAILY_DETECTOR_COUNT_CHECK as (
    select
        MHDSDC.*,
        CDDC.ML_HV_CLEARINGHOUSE_DETECTOR_COUNT,
        IDDC.ML_HV_IMPUTATION_DETECTOR_COUNT,
        PDDC.ML_HV_PERFORMANCE_DETECTOR_COUNT,
        DSC.GOOD_STATUS_COUNT,
        DSC.BAD_STATUS_COUNT,
        DSC.ALL_DETECTOR_STATUS_TOTAL_COUNT
    from ML_HV_DETECTOR_STATUS_DAILY_COUNT as MHDSDC
    left join ML_HV_CLEARINGHOUSE_DETECTOR_DAILY_COUNT as CDDC
        on MHDSDC.SAMPLE_DATE = CDDC.SAMPLE_DATE
    left join ML_HV_IMPUTATION_DETECTOR_DAILY_COUNT as IDDC
        on MHDSDC.SAMPLE_DATE = IDDC.SAMPLE_DATE
    left join ML_HV_PERFORMANCE_DETECTOR_DAILY_COUNT as PDDC
        on MHDSDC.SAMPLE_DATE = PDDC.SAMPLE_DATE
    left join DETECTOR_STATUS_COUNT as DSC
        on MHDSDC.SAMPLE_DATE = DSC.SAMPLE_DATE
)

select * from DAILY_DETECTOR_COUNT_CHECK

with

ML_HV_DETECTOR_STATUS_DAILY_COUNT as (
    /*
    * This CTE returns the number of total rows by station created daily in
    * the int_diagnostics__detector_status model. This count should be
    * to checked against the daily station counts for the following models:
    * - int_vds__detector_agg_five_minutes_normalized
    * - int_imputation__detector_imputed_agg_five_minutes
    * - int_performance__station_metrics_agg_five_minutes
    * The daily station counts for these models should match for HV and ML
    * station types
    */
    select
        SAMPLE_DATE,
        count(distinct STATION_ID) as ML_HV_DETECTOR_STATUS_STATION_COUNT
    from {{ ref('int_diagnostics__detector_status') }}
    where
        STATION_TYPE in ('ML', 'HV')
        and SAMPLE_DATE >= current_date - 16
    group by SAMPLE_DATE
),

-- Clearinghouse Station Count per Day
ML_HV_CLEARINGHOUSE_STATION_DAILY_COUNT as (
    select
        SAMPLE_DATE,
        count(distinct STATION_ID) as ML_HV_CLEARINGHOUSE_STATION_COUNT
    from {{ ref('int_vds__detector_agg_five_minutes_normalized') }}
    where
        STATION_TYPE in ('ML', 'HV')
        and SAMPLE_DATE >= current_date - 16
    group by SAMPLE_DATE
),

-- Imputation Station Count per Day
ML_HV_IMPUTATION_STATION_DAILY_COUNT as (
    select
        SAMPLE_DATE,
        count(distinct STATION_ID) as ML_HV_IMPUTATION_STATION_COUNT
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where
        STATION_TYPE in ('ML', 'HV')
        and SAMPLE_DATE >= current_date - 16
    group by SAMPLE_DATE
),

-- Performance Station Count per Day
ML_HV_PERFORMANCE_STATION_DAILY_COUNT as (
    select
        SAMPLE_DATE,
        count(distinct STATION_ID) as ML_HV_PERFORMANCE_STATION_COUNT
    from {{ ref('int_performance__station_metrics_agg_five_minutes') }}
    where
        STATION_TYPE in ('ML', 'HV')
        and SAMPLE_DATE >= current_date - 16
    group by SAMPLE_DATE
),

DAILY_STATION_COUNT_CHECK as (
    select
        MHDSDC.*,
        CSDC.ML_HV_CLEARINGHOUSE_STATION_COUNT,
        ISDC.ML_HV_IMPUTATION_STATION_COUNT,
        PSDC.ML_HV_PERFORMANCE_STATION_COUNT
    from ML_HV_DETECTOR_STATUS_DAILY_COUNT as MHDSDC
    left join ML_HV_CLEARINGHOUSE_STATION_DAILY_COUNT as CSDC
        on MHDSDC.SAMPLE_DATE = CSDC.SAMPLE_DATE
    left join ML_HV_IMPUTATION_STATION_DAILY_COUNT as ISDC
        on MHDSDC.SAMPLE_DATE = ISDC.SAMPLE_DATE
    left join ML_HV_PERFORMANCE_STATION_DAILY_COUNT as PSDC
        on MHDSDC.SAMPLE_DATE = PSDC.SAMPLE_DATE
)

select * from DAILY_STATION_COUNT_CHECK

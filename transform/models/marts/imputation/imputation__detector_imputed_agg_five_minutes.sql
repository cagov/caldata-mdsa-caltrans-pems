with imputation_five_mins as (
    select
        * exclude (sample_timestamp, station_valid_from, station_valid_to),
        sample_timestamp::timestamp_ntz(6) as sample_timestamp -- iceberg needs ms for timestamps
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where
        station_type in ('ML', 'HV')
        and {{ make_model_incremental('sample_date') }}
),

imputation_five_minsc as (
    {{ get_county_name('imputation_five_mins') }}
)

select * from imputation_five_minsc







with validation_errors as (

    select
        SAMPLE_TIMESTAMP, DETECTOR_ID
    from ANALYTICS_PRD.imputation.imputation__detector_imputed_agg_five_minutes
    group by SAMPLE_TIMESTAMP, DETECTOR_ID
    having count(*) > 1

)

select *
from validation_errors









with validation_errors as (

    select
        SAMPLE_DATE, DETECTOR_ID
    from ANALYTICS_PRD.imputation.imputation__detector_summary
    group by SAMPLE_DATE, DETECTOR_ID
    having count(*) > 1

)

select *
from validation_errors



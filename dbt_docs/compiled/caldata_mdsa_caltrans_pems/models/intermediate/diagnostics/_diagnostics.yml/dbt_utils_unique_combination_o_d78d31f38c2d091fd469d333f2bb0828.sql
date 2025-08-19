





with validation_errors as (

    select
        SAMPLE_DATE, DETECTOR_ID
    from ANALYTICS_PRD.diagnostics.int_diagnostics__samples_per_detector
    group by SAMPLE_DATE, DETECTOR_ID
    having count(*) > 1

)

select *
from validation_errors









with validation_errors as (

    select
        SAMPLE_DATE, DETECTOR_ID
    from ANALYTICS_PRD.diagnostics.int_diagnostics__detector_status
    group by SAMPLE_DATE, DETECTOR_ID
    having count(*) > 1

)

select *
from validation_errors



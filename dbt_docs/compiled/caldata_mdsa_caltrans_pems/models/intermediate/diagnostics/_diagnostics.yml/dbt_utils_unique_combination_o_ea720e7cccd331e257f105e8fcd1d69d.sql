





with validation_errors as (

    select
        ACTIVE_DATE, DETECTOR_ID
    from ANALYTICS_PRD.diagnostics.int_diagnostics__no_data_status
    group by ACTIVE_DATE, DETECTOR_ID
    having count(*) > 1

)

select *
from validation_errors



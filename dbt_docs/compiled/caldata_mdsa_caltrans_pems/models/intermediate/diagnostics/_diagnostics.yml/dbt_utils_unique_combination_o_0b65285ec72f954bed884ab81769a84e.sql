





with validation_errors as (

    select
        station_id, lane, sample_date
    from ANALYTICS_PRD.diagnostics.int_diagnostics__real_detector_status
    group by station_id, lane, sample_date
    having count(*) > 1

)

select *
from validation_errors



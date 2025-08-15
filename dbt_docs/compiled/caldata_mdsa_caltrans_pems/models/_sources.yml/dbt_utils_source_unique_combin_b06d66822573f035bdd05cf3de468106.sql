





with validation_errors as (

    select
        detector_id, time_id
    from RAW_PRD.db96.detector_config_log
    group by detector_id, time_id
    having count(*) > 1

)

select *
from validation_errors



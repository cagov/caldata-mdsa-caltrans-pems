





with validation_errors as (

    select
        controller_id, time_id
    from RAW_PRD.db96.controller_config_log
    group by controller_id, time_id
    having count(*) > 1

)

select *
from validation_errors



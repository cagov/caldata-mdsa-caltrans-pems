





with validation_errors as (

    select
        station_id, time_id
    from RAW_PRD.db96.station_config_log
    group by station_id, time_id
    having count(*) > 1

)

select *
from validation_errors



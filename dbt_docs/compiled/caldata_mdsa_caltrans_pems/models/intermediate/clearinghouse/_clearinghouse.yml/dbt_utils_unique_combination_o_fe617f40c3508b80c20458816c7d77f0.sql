





with validation_errors as (

    select
        META_DATE, DETECTOR_ID
    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__station_status
    group by META_DATE, DETECTOR_ID
    having count(*) > 1

)

select *
from validation_errors



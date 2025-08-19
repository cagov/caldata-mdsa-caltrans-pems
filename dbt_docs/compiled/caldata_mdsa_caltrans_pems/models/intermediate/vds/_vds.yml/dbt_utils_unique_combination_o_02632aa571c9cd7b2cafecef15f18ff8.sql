





with validation_errors as (

    select
        STATION_ID, _VALID_TO
    from ANALYTICS_PRD.vds.int_vds__station_config
    group by STATION_ID, _VALID_TO
    having count(*) > 1

)

select *
from validation_errors



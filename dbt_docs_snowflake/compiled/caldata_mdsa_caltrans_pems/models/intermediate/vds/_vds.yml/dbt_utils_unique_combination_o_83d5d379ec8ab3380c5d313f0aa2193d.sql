





with validation_errors as (

    select
        CONTROLLER_ID, _VALID_TO
    from ANALYTICS_PRD.vds.int_vds__controller_config
    group by CONTROLLER_ID, _VALID_TO
    having count(*) > 1

)

select *
from validation_errors



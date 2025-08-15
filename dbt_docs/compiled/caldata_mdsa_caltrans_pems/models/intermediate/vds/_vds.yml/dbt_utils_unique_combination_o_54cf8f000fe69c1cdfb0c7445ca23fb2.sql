





with validation_errors as (

    select
        DETECTOR_ID, _VALID_TO
    from ANALYTICS_PRD.vds.int_vds__detector_config
    group by DETECTOR_ID, _VALID_TO
    having count(*) > 1

)

select *
from validation_errors



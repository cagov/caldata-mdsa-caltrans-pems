
    
    

with all_values as (

    select
        DISTRICT as value_field,
        count(*) as n_records

    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__station_status
    group by DISTRICT

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



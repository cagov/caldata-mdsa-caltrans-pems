
    
    

with all_values as (

    select
        DIRECTION as value_field,
        count(*) as n_records

    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__station_meta
    group by DIRECTION

)

select *
from all_values
where value_field not in (
    'N','E','S','W','n','e','s','w'
)




    
    

with all_values as (

    select
        DIRECTION as value_field,
        count(*) as n_records

    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__detector_agg_five_minutes_with_missing_rows
    group by DIRECTION

)

select *
from all_values
where value_field not in (
    'N','E','S','W','n','e','s','w'
)



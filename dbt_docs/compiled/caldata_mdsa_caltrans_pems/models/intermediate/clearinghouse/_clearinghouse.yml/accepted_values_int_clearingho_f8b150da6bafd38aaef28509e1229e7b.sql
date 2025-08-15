
    
    

with all_values as (

    select
        direction as value_field,
        count(*) as n_records

    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__detector_outlier_agg_five_minutes
    group by direction

)

select *
from all_values
where value_field not in (
    'N','E','S','W','n','e','s','w'
)



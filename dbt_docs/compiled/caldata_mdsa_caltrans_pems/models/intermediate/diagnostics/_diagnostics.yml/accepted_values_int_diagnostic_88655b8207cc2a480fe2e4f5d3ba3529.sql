
    
    

with all_values as (

    select
        district as value_field,
        count(*) as n_records

    from ANALYTICS_PRD.diagnostics.int_diagnostics__det_diag_set_assignment
    group by district

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



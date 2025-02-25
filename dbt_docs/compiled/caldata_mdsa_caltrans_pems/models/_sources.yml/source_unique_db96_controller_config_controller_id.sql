
    
    

select
    controller_id as unique_field,
    count(*) as n_records

from RAW_PRD.db96.controller_config
where controller_id is not null
group by controller_id
having count(*) > 1



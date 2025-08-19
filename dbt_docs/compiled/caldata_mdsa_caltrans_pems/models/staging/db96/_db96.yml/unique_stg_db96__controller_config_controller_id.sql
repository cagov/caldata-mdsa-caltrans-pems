
    
    

select
    controller_id as unique_field,
    count(*) as n_records

from ANALYTICS_PRD.db96.stg_db96__controller_config
where controller_id is not null
group by controller_id
having count(*) > 1



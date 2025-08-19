
    
    

select
    detector_id as unique_field,
    count(*) as n_records

from ANALYTICS_PRD.db96.stg_db96__detector_config
where detector_id is not null
group by detector_id
having count(*) > 1



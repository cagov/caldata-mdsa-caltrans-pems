
    
    

select
    detector_id as unique_field,
    count(*) as n_records

from RAW_PRD.db96.detector_config
where detector_id is not null
group by detector_id
having count(*) > 1



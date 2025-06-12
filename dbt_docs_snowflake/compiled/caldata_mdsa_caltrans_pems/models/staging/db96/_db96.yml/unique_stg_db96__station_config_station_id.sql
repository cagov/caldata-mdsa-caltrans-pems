
    
    

select
    station_id as unique_field,
    count(*) as n_records

from ANALYTICS_PRD.db96.stg_db96__station_config
where station_id is not null
group by station_id
having count(*) > 1



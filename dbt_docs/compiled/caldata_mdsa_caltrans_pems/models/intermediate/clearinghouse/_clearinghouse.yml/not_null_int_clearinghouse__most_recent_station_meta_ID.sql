
    
    



with __dbt__cte__int_clearinghouse__most_recent_station_meta as (


with station_meta as (
    select * from ANALYTICS_PRD.clearinghouse.int_clearinghouse__station_meta
),

most_recent_station_meta as (
    select * exclude (filename, _valid_from, _valid_to)
    from station_meta
    where _valid_to is null
)

select * from most_recent_station_meta
) select ID
from __dbt__cte__int_clearinghouse__most_recent_station_meta
where ID is null



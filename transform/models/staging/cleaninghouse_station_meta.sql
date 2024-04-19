select *
from  {{ source('clearinghouse', 'station_meta') }}
limit 10
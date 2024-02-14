select FLOW_1
from {{ source('clearinghouse', 'STATION_RAW' ) }}
where FLOW_1 >= 0 or FLOW_1 is null

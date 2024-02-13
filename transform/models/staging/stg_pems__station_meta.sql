select
*
FROM {{source('CLEARINGHOUSE', 'STATION_META')}}
-- limit 10
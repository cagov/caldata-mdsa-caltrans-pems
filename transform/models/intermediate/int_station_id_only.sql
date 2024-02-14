with
source as (
    select * from {{ source('CLEARINGHOUSE', 'STATION_RAW') }}
   -- select * from stg_pems__station_meta
),

select * from source
limit 10

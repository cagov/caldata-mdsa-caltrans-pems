{{ config(materialized="table") }}

-- station_status as (
--     select 
--         detector_id,
--         station_id,
--         district,
--         len(lane_number) as lane
--     from {{ ref('int_clearinghouse__most_recent_station_status') }}
-- )

select * from {{ ref('int_clearinghouse__most_recent_station_status') }}

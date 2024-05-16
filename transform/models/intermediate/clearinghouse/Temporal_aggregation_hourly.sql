{{config(materialized='table')}}

-- read the volume, occupancy and speed five minutes data
with station_flow as {{ ref('int_performance__five_min_perform_metrics') }}


/*
This model currently uses a maximum capacity of 2076 vehicles/lane/hour for all detectors.
Using this value provides a consistent metric for determining the capcacity and
calculating the lost productivity.

The current methodology in PeMS uses the max of either the 15 minute historical highest
flow or 2076 v/l/h as the capacity at each location per PeMS website:
https://pems.dot.ca.gov/?dnode=Help&content=help_calc#perf

The issue with the current methodology is that there is no documentation for how the historical
measured maximum capacity has been developed and we have observed inconsistencies in high flow
values in PeMS. We also do not have any documention on why a 15-minute timeframe was selected
and how that value is used to compute the 5-minute aggregation level of the lost productivity
performance metric.
*/

with

source as (
    select *
    from {{ ref('int_vds__detector_config') }}
),

max_capacity_detector as (
    select distinct
        detector_id,
        /*
        2076 v/l/h / 12 = 173 v/l/5-min
        */
        173 as max_capacity_5min
    from source
    group by detector_id
)

select * from max_capacity_detector

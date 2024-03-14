-- # of samples < 60% of the max collected samples during the test period.
-- max value is a static value, which should be two samples per minute times 60 mins/hr times 17 hours in a day 
-- (1224 is the static number of samples) - 
-- this max value doesnt need to be calculated at the station_id level bc it's the same
-- btwn 1 and 1224 is too few samples.

with
diagnostic_samples_per_station as (
    select * from {{ ref("int_pems__diagnostic_samples_per_station") }}
)

select
    sample_date,
    station_id,
    coalesce(lane1_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane1_too_few_samples,
    coalesce(lane2_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane2_too_few_samples,
    coalesce(lane3_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane3_too_few_samples,
    coalesce(lane4_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane4_too_few_samples,
    coalesce(lane5_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane5_too_few_samples,
    coalesce(lane6_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane6_too_few_samples,
    coalesce(lane7_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane7_too_few_samples,
    coalesce(lane8_sample_cnt between 1 and (0.6 * (2 * 60 * 17)), false) as lane8_too_few_samples

from diagnostic_samples_per_station
group by all

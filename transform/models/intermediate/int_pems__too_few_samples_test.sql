-- # of samples < 60% of the max collected samples during the test period.
-- max value is a static value, which should be two samples per minute times 60 mins/hr times 17 hours in a day 
-- (1224 is the static number of samples) - 
-- this max value doesnt need to be calculated at the station_id level bc it's the same
-- btwn 1 and 1224 is too few samples.

with
diagnostic_samples_per_station as (
    select * from {{ ref("int_pems__diagnostic_samples_per_station") }}
),

lane1_too_few_samples as (
    select lane1_sample_cnt

    from diagnostic_samples_per_station
    where lane1_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
),

lane2_too_few_samples as (
    select lane2_sample_cnt

    from diagnostic_samples_per_station
    where lane2_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
),

lane3_too_few_samples as (
    select lane3_sample_cnt

    from diagnostic_samples_per_station
    where lane3_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
),

lane4_too_few_samples as (
    select lane4_sample_cnt

    from diagnostic_samples_per_station
    where lane4_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
),

lane5_too_few_samples as (
    select lane5_sample_cnt

    from diagnostic_samples_per_station
    where lane5_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
),

lane6_too_few_samples as (
    select lane6_sample_cnt

    from diagnostic_samples_per_station
    where lane6_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
)

-- , lane7_too_few_samples as (
--     select lane7_sample_cnt

--     from diagnostic_samples_per_station
--     where lane7_sample_cnt between 1 and (0.6 * (2 * 60 * 17))
-- )

-- , lane8_too_few_samples as (
-- select LANE8_SAMPLE_CNT

-- from diagnostic_samples_per_station
-- where LANE8_SAMPLE_CNT between 1 and (0.6*(2*60*17))
-- )

select
    lane1_too_few_samples.*,
    lane2_too_few_samples.*,
    lane3_too_few_samples.*,
    lane4_too_few_samples.*,
    lane5_too_few_samples.*,
    lane6_too_few_samples.*
-- , lane7_too_few_samples.* 
-- , lane8_too_few_samples.*  -- currently lane 7 and lane 8 causes the query to produce no results!

from lane1_too_few_samples,
    lane2_too_few_samples,
    lane3_too_few_samples,
    lane4_too_few_samples,
    lane5_too_few_samples,
    lane6_too_few_samples
-- , lane7_too_few_samples 
-- , lane8_too_few_samples -- currently lane 7 and lane 8 causes the query to produce no results!
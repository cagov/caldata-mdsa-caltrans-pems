/*
This SQL file will be used to count the number of 30 second raw data samples recieved
for a variety of conditions by station, lane and date. This data will be used to run
detector diagnostics and see if a detector is good or bad based on criteria established
for the PeMS application. In this file we look at a specific date (in this case 2023-01-01)
for testing. For Production this date will need to count samples for the previous day.
This count should only be done after a complete days worth of raw data has been recieved
so the suggested interval should be once a day some time after midnight which should incude
anytime needed for the final sample of the previous day to be sent and recieved.
*/
select
    ID as STATION_ID,
    SAMPLE_TIMESTAMP,
    /*
    This code counts a sample if the flow and occupancy values contain any value
    based on 30 second raw data recieved per station, lane and time. Null values
    in flow and occupancy are currently counted as 0 but if these need to be treated
    differently below is a code example that may be useful if needed in the future
    when FLOW_1 is null and OCCUPANCY_1 is null then null
    */
    case
        when FLOW_1 is not null and OCCUPANCY_1 is not null then 1
        else 0
    end as LANE_1_COUNT,
    case
        when FLOW_2 is not null and OCCUPANCY_2 is not null then 1
        else 0
    end as LANE_2_COUNT,
    case
        when FLOW_3 is not null and OCCUPANCY_3 is not null then 1
        else 0
    end as LANE_3_COUNT,
    case
        when FLOW_4 is not null and OCCUPANCY_4 is not null then 1
        else 0
    end as LANE_4_COUNT,
    case
        when FLOW_5 is not null and OCCUPANCY_5 is not null then 1
        else 0
    end as LANE_5_COUNT,
    case
        when FLOW_6 is not null and OCCUPANCY_6 is not null then 1
        else 0
    end as LANE_6_COUNT,
    case
        when FLOW_7 is not null and OCCUPANCY_7 is not null then 1
        else 0
    end as LANE_7_COUNT,
    case
        when FLOW_8 is not null and OCCUPANCY_8 is not null then 1
        else 0
    end as LANE_8_COUNT,
    /*
    The following code will count how many times a 30 second raw flow value equals 0
    for a given station and associated lane
    */
    case
        when FLOW_1 = 0 then 1
        else 0
    end as LANE_1_ZEROFLOWCOUNT,
    case
        when FLOW_2 = 0 then 1
        else 0
    end as LANE_2_ZEROFLOWCOUNT,
    case
        when FLOW_3 = 0 then 1
        else 0
    end as LANE_3_ZEROFLOWCOUNT,
    case
        when FLOW_4 = 0 then 1
        else 0
    end as LANE_4_ZEROFLOWCOUNT,
    case
        when FLOW_5 = 0 then 1
        else 0
    end as LANE_5_ZEROFLOWCOUNT,
    case
        when FLOW_6 = 0 then 1
        else 0
    end as LANE_6_ZEROFLOWCOUNT,
    case
        when FLOW_7 = 0 then 1
        else 0
    end as LANE_7_ZEROFLOWCOUNT,
    case
        when FLOW_8 = 0 then 1
        else 0
    end as LANE_8_ZEROFLOWCOUNT,
    /*
    The following code will count how many times a 30 second raw occupancy value equals 0
    for a given station and associated lane
    */
    case
        when OCCUPANCY_1 = 0 then 1
        else 0
    end as LANE_1_ZEROOCCCOUNT,
    case
        when OCCUPANCY_2 = 0 then 1
        else 0
    end as LANE_2_ZEROOCCCOUNT,
    case
        when OCCUPANCY_3 = 0 then 1
        else 0
    end as LANE_3_ZEROOCCCOUNT,
    case
        when OCCUPANCY_4 = 0 then 1
        else 0
    end as LANE_4_ZEROOCCCOUNT,
    case
        when OCCUPANCY_5 = 0 then 1
        else 0
    end as LANE_5_ZEROOCCCOUNT,
    case
        when OCCUPANCY_6 = 0 then 1
        else 0
    end as LANE_6_ZEROOCCCOUNT,
    case
        when OCCUPANCY_7 = 0 then 1
        else 0
    end as LANE_7_ZEROOCCCOUNT,
    case
        when OCCUPANCY_8 = 0 then 1
        else 0
    end as LANE_8_ZEROOCCCOUNT
/*
The following code will count how many times a 30 second raw occupancy value is
considered HIGH. The HIGH value used for comparison comes from the DIAGNOSTIC_THRESHOLD_VALUES
table and is based on the stations threshold set, detector set ID
(Urban_D11, Low_Volume, Urban, Rural, D6_Ramps as of 2/22/2024) and type (typically
ML, HOV or Ramp).
*/

from {{ source("CLEARINGHOUSE", "STATION_RAW") }}

where DATE(SAMPLE_TIMESTAMP) = '2023-01-01'

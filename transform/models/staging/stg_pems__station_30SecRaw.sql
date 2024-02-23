/*
This SQL file will be used to count the number of 30 second raw data samples recieved
for a variety of conditions by station, lane and date. This data will be used to run
detector diagnostics and see if a detector is good or bad based on criteria established
for the PeMS application. In this file we look at the same day in the previous year since
the previous days data is not currently updating at the frequency needed (as of 2/23/24).
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
    differently the code should be updated as needed to accomodate such a scenario.
    */
    case
        when FLOW_1 is not null and OCCUPANCY_1 is not null then 1
        else 0
    end as LANE_1_CNT,
    case
        when FLOW_2 is not null and OCCUPANCY_2 is not null then 1
        else 0
    end as LANE_2_CNT,
    case
        when FLOW_3 is not null and OCCUPANCY_3 is not null then 1
        else 0
    end as LANE_3_CNT,
    case
        when FLOW_4 is not null and OCCUPANCY_4 is not null then 1
        else 0
    end as LANE_4_CNT,
    case
        when FLOW_5 is not null and OCCUPANCY_5 is not null then 1
        else 0
    end as LANE_5_CNT,
    case
        when FLOW_6 is not null and OCCUPANCY_6 is not null then 1
        else 0
    end as LANE_6_CNT,
    case
        when FLOW_7 is not null and OCCUPANCY_7 is not null then 1
        else 0
    end as LANE_7_CNT,
    case
        when FLOW_8 is not null and OCCUPANCY_8 is not null then 1
        else 0
    end as LANE_8_CNT,
    /*
    The following code will count how many times a 30 second raw flow value equals 0
    for a given station and associated lane
    */
    case
        when FLOW_1 = 0 then 1
        else 0
    end as LANE_1_ZEROFLOWCNT,
    case
        when FLOW_2 = 0 then 1
        else 0
    end as LANE_2_ZEROFLOWCNT,
    case
        when FLOW_3 = 0 then 1
        else 0
    end as LANE_3_ZEROFLOWCNT,
    case
        when FLOW_4 = 0 then 1
        else 0
    end as LANE_4_ZEROFLOWCNT,
    case
        when FLOW_5 = 0 then 1
        else 0
    end as LANE_5_ZEROFLOWCNT,
    case
        when FLOW_6 = 0 then 1
        else 0
    end as LANE_6_ZEROFLOWCNT,
    case
        when FLOW_7 = 0 then 1
        else 0
    end as LANE_7_ZEROFLOWCNT,
    case
        when FLOW_8 = 0 then 1
        else 0
    end as LANE_8_ZEROFLOWCNT,
    /*
    The following code will count how many times a 30 second raw occupancy value equals 0
    for a given station and associated lane
    */
    case
        when OCCUPANCY_1 = 0 then 1
        else 0
    end as LANE_1_ZEROOCCCNT,
    case
        when OCCUPANCY_2 = 0 then 1
        else 0
    end as LANE_2_ZEROOCCCNT,
    case
        when OCCUPANCY_3 = 0 then 1
        else 0
    end as LANE_3_ZEROOCCCNT,
    case
        when OCCUPANCY_4 = 0 then 1
        else 0
    end as LANE_4_ZEROOCCCNT,
    case
        when OCCUPANCY_5 = 0 then 1
        else 0
    end as LANE_5_ZEROOCCCNT,
    case
        when OCCUPANCY_6 = 0 then 1
        else 0
    end as LANE_6_ZEROOCCCNT,
    case
        when OCCUPANCY_7 = 0 then 1
        else 0
    end as LANE_7_ZEROOCCCNT,
    case
        when OCCUPANCY_8 = 0 then 1
        else 0
    end as LANE_8_ZEROOCCCNT,
    /*
    This code counts a sample if the flow is 0 and occupancy value > 0
    based on 30 second raw data recieved per station, lane and time.
    */
    case
        when FLOW_1 = 0 and OCCUPANCY_1 > 0 then 1
        else 0
    end as LANE_1_ZF_NNO_CNT,
    case
        when FLOW_2 = 0 and OCCUPANCY_2 > 0 then 1
        else 0
    end as LANE_2_ZF_NNO_CNT,
    case
        when FLOW_3 = 0 and OCCUPANCY_3 > 0 then 1
        else 0
    end as LANE_3_ZF_NNO_CNT,
    case
        when FLOW_4 = 0 and OCCUPANCY_4 > 0 then 1
        else 0
    end as LANE_4_ZF_NNO_CNT,
    case
        when FLOW_5 = 0 and OCCUPANCY_5 > 0 then 1
        else 0
    end as LANE_5_ZF_NNO_CNT,
    case
        when FLOW_6 = 0 and OCCUPANCY_6 > 0 then 1
        else 0
    end as LANE_6_ZF_NNO_CNT,
    case
        when FLOW_7 = 0 and OCCUPANCY_7 > 0 then 1
        else 0
    end as LANE_7_ZF_NNO_CNT,
    case
        when FLOW_8 = 0 and OCCUPANCY_8 > 0 then 1
        else 0
    end as LANE_8_ZF_NNO_CNT,
    /*
    This code counts a sample if the occupancy is 0 and a flow value > 0
    based on 30 second raw data recieved per station, lane and time.
    */
    case
        when FLOW_1 > 0 and OCCUPANCY_1 = 0 then 1
        else 0
    end as LANE_1_NNF_ZO_CNT,
    case
        when FLOW_2 > 0 and OCCUPANCY_2 = 0 then 1
        else 0
    end as LANE_2_NNF_ZO_CNT,
    case
        when FLOW_3 > 0 and OCCUPANCY_3 = 0 then 1
        else 0
    end as LANE_3_NNF_ZO_CNT,
    case
        when FLOW_4 > 0 and OCCUPANCY_4 = 0 then 1
        else 0
    end as LANE_4_NNF_ZO_CNT,
    case
        when FLOW_5 > 0 and OCCUPANCY_5 = 0 then 1
        else 0
    end as LANE_5_NNF_ZO_CNT,
    case
        when FLOW_6 > 0 and OCCUPANCY_6 = 0 then 1
        else 0
    end as LANE_6_NNF_ZO_CNT,
    case
        when FLOW_7 > 0 and OCCUPANCY_7 = 0 then 1
        else 0
    end as LANE_7_NNF_ZO_CNT,
    case
        when FLOW_8 > 0 and OCCUPANCY_8 = 0 then 1
        else 0
    end as LANE_8_NNF_ZO_CNT
/*
To be developed: Code that will count how many times a 30 second raw occupancy value is
considered HIGH. The HIGH value used for comparison comes from the DIAGNOSTIC_THRESHOLD_VALUES
table and is based on the stations threshold set, detector set ID
(Urban_D11, Low_Volume, Urban, Rural, D6_Ramps as of 2/22/2024) and type (typically
ML, HOV or Ramp).
*/

from {{ source("CLEARINGHOUSE", "STATION_RAW") }}
/*
Currently looks at the same day in previous year. This should be updated to
look at the previous day once the data refresh brings in more current data
*/
where date(SAMPLE_TIMESTAMP) = dateadd(year, -1, current_date())

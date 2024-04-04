with

source as (
    select * from {{ ref ('stg_clearinghouse__station_raw') }}
    where
        TO_TIME(sample_timestamp) >= '05:00:00' and TO_TIME(sample_timestamp) <= '21:59:59'
        and sample_date = DATEADD('day', -2, CURRENT_DATE())
),

samples_per_station as (
    select
        set_assgnmt.district,
        set_assgnmt.station_id as station_id,
        set_assgnmt.lane_num,
        COALESCE(source.sample_date, DATEADD('day', -2, CURRENT_DATE())) as sample_count_date,
        /*
        This following counts a sample if the volume (flow) and occupancy values contain any value
        based on 30 second raw data recieved per station, lane and time. Null values
        in volume (flow) and occupancy are currently counted as 0 but if these need to be treated
        differently the code should be updated as needed to accomodate such a scenario.
        */
        COUNT_IF(source.volume is not null and source.occupancy is not null) as sample_ct,

        /*
        The following code will count how many times a 30 second raw volume (flow) value equals 0
        for a given station and associated lane
        */
        COUNT_IF(source.volume = 0) as zero_vol_ct,

        /*
        The following code will count how many times a 30 second raw occupancy value equals 0
        for a given station and associated lane
        */
        COUNT_IF(source.occupancy = 0) as zero_occ_ct,

        /*
        This code counts a sample if the volume (flow) is 0 and occupancy value > 0
        based on 30 second raw data recieved per station, lane, and time.
        */
        COUNT_IF(source.volume = 0 and source.occupancy > 0) as zero_vol_pos_occ_ct,

        /*
        This code counts a sample if the occupancy is 0 and a volume (flow) value > 0
        based on 30 second raw data recieved per station, lane and time.
        */
        COUNT_IF(source.volume > 0 and source.occupancy = 0) as zero_occ_pos_vol_ct,

        /*
        This SQL file counts the number of volume (flow) and occupancy values that exceed
        detector threshold values for a station based on the station set assignment.
        */
        COUNT_IF(source.volume > set_assgnmt.high_flow) as high_volume_ct,
        COUNT_IF(source.occupancy > set_assgnmt.high_occupancy) as high_occupancy_ct

    from {{ ref('int_pems__det_diag_set_assignment') }} as set_assgnmt
    left join source
        on
            source.id = set_assgnmt.station_id
            and source.lane = set_assgnmt.lane_num
    --Look only at the previous day to determine a detectors status
    --for development we use 2 days but the final code should be updated
    --from -2 to -1
    --where
    --    source.sample_date = DATEADD('day', -2, CURRENT_DATE())
    --    or source.sample_date is null
    group by set_assgnmt.district, set_assgnmt.station_id, set_assgnmt.lane_num, sample_count_date
)

select * from samples_per_station

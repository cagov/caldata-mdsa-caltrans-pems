with

source as (
    select *
    from {{ ref ('int_clearinghouse__detector_agg_five_minutes') }}
    where
        TO_TIME(sample_timestamp) >= {{ var("day_start") }}
        and TO_TIME(sample_timestamp) <= {{ var("day_end") }}
        and {{ make_model_incremental('sample_date') }}
),

samples_per_station as (
    select
        district,
        id as station_id,
        lane,
        sample_date,
        /*
        This following counts a sample if the volume (flow) and occupancy values contain any value
        based on 30 second raw data recieved per station, lane and time. Null values
        in volume (flow) and occupancy are currently counted as 0 but if these need to be treated
        differently the code should be updated as needed to accomodate such a scenario.
        */
        sample_ct,

        /*
        The following code will count how many times a 30 second raw volume (flow) value equals 0
        for a given station and associated lane
        */
        zero_vol_ct,

        /*
        The following code will count how many times a 30 second raw occupancy value equals 0
        for a given station and associated lane
        */
        zero_occ_ct,

        /*
        This code counts a sample if the volume (flow) is 0 and occupancy value > 0
        based on 30 second raw data recieved per station, lane, and time.
        */
        zero_vol_pos_occ_ct,

        /*
        This code counts a sample if the occupancy is 0 and a volume (flow) value > 0
        based on 30 second raw data recieved per station, lane and time.
        */
        zero_occ_pos_vol_ct,

        /*
        This SQL file counts the number of volume (flow) and occupancy values that exceed
        detector threshold values for a station based on the station set assignment. For
        processing optimization a high flow value or 20 and high occupancy value of 0.7
        have been hardcoded in the formulas below to avoid joining the set assignment model
        */
        high_volume_ct,
        high_occupancy_ct

    from source

)

select * from samples_per_station

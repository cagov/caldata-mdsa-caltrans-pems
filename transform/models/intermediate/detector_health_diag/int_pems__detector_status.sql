{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['station_id', 'sample_date', 'lane'],
    snowflake_warehouse="transforming_xl_dev"
) }}

with
source as (
    select * from {{ ref ('stg_clearinghouse__station_raw') }}
    where
        TO_TIME(sample_timestamp) >= '05:00:00' and TO_TIME(sample_timestamp) <= '21:59:59'
    {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select DATEADD(day, -2, MAX(sample_date)) from {{ this }}
            )
    {% endif %}
    {% if target.name != 'prd' %}
            and sample_date = DATEADD('day', -2, CURRENT_DATE())
    {% endif %}
),

samples_per_station as (
    select
        set_assgnmt.station_id as station_id,
        source.lane,
        source.sample_date,
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
        on source.id = set_assgnmt.station_id
    --Look only at the previous day to determine a detectors status
    --for development we use 2 days but the final code should be updated
    --from -2 to -1
    where
        source.sample_date = DATEADD('day', -2, CURRENT_DATE())
        or source.sample_date is null
    group by set_assgnmt.station_id, source.lane, source.sample_date
),

detector_status as (
    select
        sps.*,
        case
            when sps.sample_ct = 0 or sps.sample_ct is null
                then 'Down/No Data'
            -- # of samples < 60% of the max collected samples during the test period
            -- max value: 2 samples per minute times 60 mins/hr times 17 hours in a day which == 1224
            -- btwn 1 and 1224 is too few samples
            when sps.sample_ct between 1 and (0.6 * (2 * 60 * 17))
                then 'Insufficient Data'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.zero_vol_ct / (2 * 60 * 17) > (set_assgnmt.zero_flow_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_ct / (2 * 60 * 17) > (set_assgnmt.zero_occupancy_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.high_volume_ct / (2 * 60 * 17) > (set_assgnmt.high_flow_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.high_occupancy_ct / (2 * 60 * 17) > (set_assgnmt.high_occupancy_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_vol_pos_occ_ct / (2 * 60 * 17) > (set_assgnmt.flow_occupancy_percent / 100)
                then 'Intermittent'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_pos_vol_ct / (2 * 60 * 17) > (set_assgnmt.occupancy_flow_percent / 100)
                then 'Intermittent'
            --constant occupancy case needed
            --Feed unstable case needed
            else 'Good'
        end as status
    from {{ ref('int_pems__det_diag_set_assignment') }} as set_assgnmt
    inner join samples_per_station as sps
        on sps.station_id = set_assgnmt.station_id
    --Look only at the previous day to determine a detectors status
    --for development we use 2 days but the final code should be updated
    --from -2 to -1
    where
        sps.sample_date = DATEADD('day', -2, CURRENT_DATE())
        or sps.sample_date is null
)

select * from detector_status

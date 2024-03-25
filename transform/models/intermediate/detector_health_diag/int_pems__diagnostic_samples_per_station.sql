{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['station_id', 'sample_date', 'lane'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with
source as (
    select * from {{ ref ('stg_clearinghouse__station_raw') }}
    where
        TO_TIME(sample_timestamp) >= '05:00:00' and TO_TIME(sample_timestamp) <= '21:59:59'
        {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select DATEADD(day, -2, max(sample_date)) from {{ this }}
            )
        {% endif %}
        {% if target.name == 'dev' %}
            and sample_date >= DATEADD('day', -14, CURRENT_DATE())
        {% endif %}
),

samples_per_station as (
    select
        source.sample_date,
        source.id as station_id,
        source.lane,
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
        COUNT_IF(source.volume > set_assgnmt.dt_value and set_assgnmt.dt_name = 'high_flow') as high_vol_ct,
        COUNT_IF(source.occupancy > set_assgnmt.dt_value and set_assgnmt.dt_name = 'high_occ') as high_occ_ct

    from source
    left join {{ ref('int_pems__det_diag_set_assignment') }} as set_assgnmt
        on source.id = set_assgnmt.station_id

    group by source.id, source.sample_date, source.lane
),

det_diag_too_few_samples as (
    select
        *,
        -- # of samples < 60% of the max collected samples during the test period
        -- max value: 2 samples per minute times 60 mins/hr times 17 hours in a day which == 1224
        -- btwn 1 and 1224 is too few samples
        COALESCE(sample_ct between 1 and (0.6 * (2 * 60 * 17)), false) as too_few_samples

    from samples_per_station
)

select * from det_diag_too_few_samples

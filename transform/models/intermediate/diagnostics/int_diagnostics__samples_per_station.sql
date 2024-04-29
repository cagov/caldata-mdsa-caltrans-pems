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
        TO_TIME(sample_timestamp) >= {{ var("day_start") }}
        and TO_TIME(sample_timestamp) <= {{ var("day_end") }}
        {% if is_incremental() %}
        and sample_date > (
                select
                    dateadd(day, {{ var("incremental_model_look_back") }}, max(sample_date))
                from {{ this }}
            )
            {% if target.name != 'prd' %}
                and sample_date
                >= dateadd('day', {{ var("dev_model_look_back") }}, current_date())
            {% endif %}
        {% elif target.name != 'prd' %}
            and sample_date
            >= DATEADD('day', {{ var("dev_model_look_back") }}, CURRENT_DATE())
        {% endif %}
),

samples_per_station as (
    select
        source.district,
        source.id as station_id,
        source.lane,
        source.sample_date,
        /*
        This following counts a sample if the volume (flow) and occupancy values contain any value
        based on 30 second raw data recieved per station, lane and time. Null values
        in volume (flow) and occupancy are currently counted as 0 but if these need to be treated
        differently the code should be updated as needed to accomodate such a scenario.
        */
        COUNT_IF(source.volume is not null and source.occupancy is not null)
            as sample_ct,

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
        COUNT_IF(source.volume = 0 and source.occupancy > 0)
            as zero_vol_pos_occ_ct,

        /*
        This code counts a sample if the occupancy is 0 and a volume (flow) value > 0
        based on 30 second raw data recieved per station, lane and time.
        */
        COUNT_IF(source.volume > 0 and source.occupancy = 0)
            as zero_occ_pos_vol_ct,

        /*
        This SQL file counts the number of volume (flow) and occupancy values that exceed
        detector threshold values for a station based on the station set assignment. For
        processing optimization a high flow value or 20 and high occupancy value of 0.7
        have been hardcoded in the formulas below to avoid joining the set assignment model
        */
        COUNT_IF(source.volume > {{ var("high_volume_threshold") }})
            as high_volume_ct,
        COUNT_IF(source.occupancy > {{ var("high_occupancy_threshold") }})
            as high_occupancy_ct

    from source
    group by
        source.district, source.id, source.lane, source.sample_date
)

select * from samples_per_station

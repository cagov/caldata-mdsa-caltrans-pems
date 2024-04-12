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
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select DATEADD(day, {{ var("incremental_model_look_back") }}, MAX(sample_date))
                from {{ this }}
            )
        {% endif %}
        {% if target.name != 'prd' %}
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
),

previous_occupancy_check as (
    select
        f.id,
        f.sample_date,
        f.occupancy,
        LAG(f.occupancy) over (order by f.sample_date) as previous_occupancy
    from {{ ref("int_clearinghouse__five_minute_station_agg") }} as f
),

constant_occupany_check as (
    select
        *,
        COUNT(occupancy = previous_occupancy)
            over (order by sample_date rows between 47 preceding and current row)
            as constant_occupancy_count
    from previous_occupancy_check
)

select
    sps.*,
    c.* exclude (id, sample_date),
    COALESCE(c.constant_occupancy_count > 28, false) as constant_occupancy
from samples_per_station as sps
inner join constant_occupany_check as c
    on sps.station_id = c.id and sps.sample_date = c.sample_date
group by all

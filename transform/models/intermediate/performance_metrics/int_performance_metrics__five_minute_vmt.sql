{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["ID", "LANE", "SAMPLE_TIMESTAMP"],
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}




with station_meta as (
    select
        ID,
        LENGTH,
        TYPE,
        LANES,
        META_DATE,
        _VALID_FROM,
        _VALID_TO
    from {{ ref('int_clearinghouse__station_meta') }}
    WHERE TYPE = "ML"

),

fivemin_agg as (
    SELECT 
        lane,
        sample_date,
        sample_timestamp,
        volume
    FROM {{ ref('int_clearinghouse__five_minute_station_agg') }}

    {% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where sample_date > (
            select dateadd(day, -2, max(sample_date)) from {{ this }}
        )
    {% endif %}
)


vmt_agg_5min as (
    select
        station_meta.ID,
        fivemin_agg.lane,
        fivemin_agg.sample_date,
        fivemin_agg.sample_timestamp,
        -- VMT calculation (5 min agg volume * station coverage length)
        fivemin_agg.volume * station_meta.LENGTH as VMT 
    from  fivemin_agg 
    left join station_meta
        where station_meta.ID = fivemin_agg.ID 
            and station_meta.Lane = fivemin_agg.lane
    group by id, lane, sample_date, sample_timestamp

)

select * from vmt_agg_5min

{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["ID", "LANE", "SAMPLE_TIMESTAMP"],
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) 
}}

with station_meta as (
    select
        id,
        length,
        type,
        lanes,
        meta_date,
        _valid_from,
        _valid_to
    from {{ ref('int_clearinghouse__station_meta') }}
    where type = 'ML'
),

fivemin_agg as (
    select
        lane,
        sample_date,
        sample_timestamp,
        volume
    from {{ ref('int_clearinghouse__five_minute_station_agg') }}

    {% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where sample_date > (
            select dateadd(day, -2, max(sample_date)) from {{ this }}
        )
    {% endif %}
),

vmt_agg_5min as (
    select
        station_meta.id,
        fivemin_agg.lane,
        fivemin_agg.sample_date,
        fivemin_agg.sample_timestamp,
        -- VMT calculation (5 min agg volume * station coverage length)
        fivemin_agg.volume * station_meta.length as vmt
    from fivemin_agg
    left join station_meta
        on
            fivemin_agg.id = station_meta.id
            and fivemin_agg.lane = station_meta.lanes
    group by station_meta.id, fivemin_agg.lane, fivemin_agg.sample_date, fivemin_agg.sample_timestamp
)

select * from vmt_agg_5min;

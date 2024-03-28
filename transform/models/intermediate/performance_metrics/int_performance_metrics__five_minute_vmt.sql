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

    {% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where sample_date > (
            select dateadd(day, -2, max(sample_date)) from {{ this }}
        )
    {% endif %}
),

vmt_aggregated as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        lane,
        sum(volume) as volume, -- Sum of all the flow values
        avg(occupancy) as occupancy -- Average of all the occupancy values

        # VMT calculation here func(fivemin_agg.flow)

    from station_meta
    left join fivemin_agg
    where station_meta.ID = fivemin_agg.ID and Lane, 

    group by id, lane, sample_date, sample_timestamp_trunc
)

select * from vmt_aggregated

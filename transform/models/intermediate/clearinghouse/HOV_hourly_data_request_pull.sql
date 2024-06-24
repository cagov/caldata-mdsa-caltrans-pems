{{ config(materialized="table") }}


-- read the hourly_data
with HOV_HOURLY_VOLUME_SPEED as (
    select
        ID,
        DISTRICT,
        LANE,
        SAMPLE_HOUR,
        LENGTH,
        SAMPLE_DATE,
        HOURLY_SPEED,
        HOURLY_VOLUME,
        TYPE
    from {{ ref('int_performance__detector_metrics_agg_hourly') }}
    where
        TYPE = 'HV'
        and DISTRICT not in (1, 2, 6, 9)
        and SAMPLE_DATE >= '2023-06-10'
        and SAMPLE_DATE < '2024-06-09'
),

-- Read the meta data
META_DATA_HOV as (
    select
        HHVS.*,
        RDS.STATUS
    from HOV_HOURLY_VOLUME_SPEED as HHVS
    inner join {{ ref("int_diagnostics__real_detector_status") }} as RDS
        on
            HHVS.ID = RDS.STATION_ID
            and HHVS.LANE = RDS.LANE
            and HHVS.SAMPLE_DATE = RDS.ACTIVE_DATE
),

--  we are still missing direction,city, county and freeway lets read the daily vol file
META_DATA_HOV_WITH_DIRECTION as (
    select
        META_DATA_HOV.*,
        DA.DIRECTION,
        DA.CITY,
        DA.COUNTY,
        DA.FREEWAY
    from META_DATA_HOV
    inner join
        {{ ref('int_performance__detector_metrics_agg_daily') }} as DA
        on
            META_DATA_HOV.ID = DA.ID
            and META_DATA_HOV.LANE = DA.LANE
            and DA.SAMPLE_DATE >= '2023-06-10'
            and DA.SAMPLE_DATE < '2024-06-09'
)

select * from META_DATA_HOV_WITH_DIRECTION

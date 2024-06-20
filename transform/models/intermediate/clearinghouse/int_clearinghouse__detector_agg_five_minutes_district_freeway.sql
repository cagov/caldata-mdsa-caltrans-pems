/* Five-minute VDS data, aggregated to district-freeway combinations.
   This is used to look at broad patterns on a given route within a
   district. */

-- TODO: make incremental
{{ config(
    materialized="table",
    snowflake_warehouse=get_snowflake_warehouse("XL")
) }}

with five_minute_detector_agg as (
    select * from {{ ref("int_clearinghouse__detector_agg_five_minutes") }}
    -- TODO: incrementality here
    where sample_date >= '2024-02-01'::date
),

/** TODO: thinking more and more we need to merge this at the five_minute_station_agg model. **/
station_meta as (
    select * from {{ ref("int_clearinghouse__station_meta") }}
    where type in ('ML', 'HV') -- TODO: do we want to do this?
),

/** TODO: filter non-operational stations **/
five_minute_detector_agg_with_meta as (
    select
        five_minute_detector_agg.*,
        station_meta.freeway,
        station_meta.direction
    from five_minute_detector_agg
    inner join station_meta
        on
            five_minute_detector_agg.id = station_meta.id
            and five_minute_detector_agg.sample_date >= station_meta._valid_from
            and (five_minute_detector_agg.sample_date < station_meta._valid_to or station_meta._valid_to is null)
),

agg_district_freeway as (
    select
        district,
        freeway,
        direction,
        sample_timestamp,
        sample_date,
        avg(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        avg(speed_weighted) as speed_weighted -- TODO: flow weighted speed here?
    from five_minute_detector_agg_with_meta
    group by sample_date, sample_timestamp, district, freeway, direction
)

select * from agg_district_freeway

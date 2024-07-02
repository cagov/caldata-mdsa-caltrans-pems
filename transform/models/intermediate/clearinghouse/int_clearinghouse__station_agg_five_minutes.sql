{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL")
) }}

with detector_agg_five_minutes as (
    select *
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

real_detector_status as (
    select
        station_id as id,
        status,
        sample_date,
        lane
    from {{ ref ("int_diagnostics__real_detector_status") }}
    where status = 'Good'
),

joined_data as (
    select d.*
    from detector_agg_five_minutes as d
    inner join real_detector_status as r
        on
            d.lane = r.lane
            and d.id = r.id
            and d.sample_date = r.sample_date
),

station_aggregated_speed as (
    select
        id,
        sample_date,
        sample_timestamp,
        district,
        sum(sample_ct) as sample_ct,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * speed_weighted) / nullifzero(sum(volume_sum)) as speed_weighted
    from joined_data
    group by id, sample_date, sample_timestamp, district
)

select * from station_aggregated_speed

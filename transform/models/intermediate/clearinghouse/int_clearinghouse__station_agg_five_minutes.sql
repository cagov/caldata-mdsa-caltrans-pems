{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with detector_agg_five_minutes as (
    select
        *
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
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
    from detector_agg_five_minutes
    group by id, sample_date, sample_timestamp, district
)

select * from station_aggregated_speed

{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["station_id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL")
) }}

with detector_agg_five_minutes as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

station_aggregated_speed as (
    select
        station_id,
        sample_date,
        sample_timestamp,
        any_value(freeway) as freeway,
        any_value(direction) as direction,
        any_value(station_type) as station_type,
        any_value(absolute_postmile) as absolute_postmile,
        any_value(district) as district,
        sum(sample_ct) as sample_ct,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
        sum(volume_sum * speed_weighted) / nullifzero(sum(volume_sum)) as speed_weighted
    from detector_agg_five_minutes
    group by station_id, sample_date, sample_timestamp
)

select * from station_aggregated_speed

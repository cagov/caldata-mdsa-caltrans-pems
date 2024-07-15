{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "sample_date", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS")
) }}

with

bottlenecks_cte as (
    select * from {{ ref ("int_performance__bottlenecks") }}
    where {{ make_model_incremental('sample_date') }}
),

spatial_extent_cte as (
    select 
        id,
        sample_date,
        sample_timestamp,
        is_bottleneck,
        sum(length) over (partition by sample_timestamp) as spatial_extent 
    from bottlenecks_cte
    qualify is_bottleneck = true
)

select * from spatial_extent_cte

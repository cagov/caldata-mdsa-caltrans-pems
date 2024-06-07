{{ config(
    materialized="table",
    cluster_by="sample_date",
    snowflake_warehouse="transforming_xl_dev",
) }}
{% set n_lanes = 14 %}

with raw as (
    select *
    from {{ ref("stg_db96__vds30sec") }}
),

{% for lane in range(1, n_lanes+1) %}
    raw_{{ lane }} as (
        select
            raw.id,
            raw.sample_date,
            raw.sample_timestamp,
            raw.district,
            {{ lane }} as lane,
            raw.flow_{{ lane }} as flow,
            raw.occupancy_{{ lane }} as occupancy,
            raw.speed_{{ lane }} as speed
        from raw
    ),
{% endfor %}

unioned as (
    {% for lane in range(1, n_lanes+1) %}
        select * from raw_{{ lane }}
        {{ "union all" if not loop.last }}
    {% endfor %}
)

select * from unioned

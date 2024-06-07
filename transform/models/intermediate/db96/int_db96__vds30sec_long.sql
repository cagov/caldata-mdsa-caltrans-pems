{{ config(materialized="table") }}
{% set n_lanes = 14 %}

with raw as (
    select *
    from {{ ref("stg_db96__vds30sec") }}
),

raw_long as (
    select
        raw.id,
        raw.sample_date,
        raw.sample_timestamp,
        raw.district,
        lane.value as lane,
        [
            {% for lane in range(1, n_lanes+1) %}
                raw.flow_{{ lane }}{{ "," if not loop.last }}
            {% endfor %}
        ][
            lane.index
        ] as flow,
        [
            {% for lane in range(1, n_lanes+1) %}
                raw.occupancy_{{ lane }}{{ "," if not loop.last }}
            {% endfor %}
        ][
            lane.index
        ] as occupancy,
        [
            {% for lane in range(1, n_lanes+1) %}
                raw.speed_{{ lane }}{{ "," if not loop.last }}
            {% endfor %}
        ][
            lane.index
        ] as speed
    from raw,
        lateral flatten([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]) as lane
)

select * from raw_long

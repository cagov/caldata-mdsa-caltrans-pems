{{ config(materialized="table") }}
{% set n_lanes = 14 %}

with raw as (
    select
        *,
        /* Create a timestamp truncated down to the nearest five
         minute bucket. This will be the the timestamp on which
         we aggregate. If a 30-second interval straddles two different
         buckets, it will be assigned to the one latter one due to
         the floor() call.
        */
        dateadd(
            'minute',
            floor(minute(sample_timestamp) / 5) * 5,
            trunc(sample_timestamp, 'hour')
        ) as sample_timestamp_trunc
    from {{ ref("stg_db96__vds30sec") }}
),

agg as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        district,
        {% for lane in range(1, n_lanes+1) %}
            sum(flow_{{ lane }}) as flow_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            avg(occupancy_{{ lane }}) as occupancy_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            avg(speed_{{ lane }}) as speed_{{ lane }}{{ "," if not loop.last }}
        {% endfor %}
    from raw
    group by id, sample_date, sample_timestamp_trunc, district
),

raw_long as (
    select
        raw.id,
        raw.sample_date,
        raw.sample_timestamp_trunc,
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
),

agg_long as (
    select
        id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        district,
        lane,
        sum(flow) as flow,
        avg(occupancy) as occupancy,
        avg(speed) as speed
    from raw_long
    group by id, sample_date, sample_timestamp_trunc, district, lane
)

select * from agg_long

{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}
{% set n_lanes = 8 %}

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
    from {{ ref('stg_clearinghouse__station_raw') }}

    where {{ make_model_incremental('sample_date') }}
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
            count_if(flow_{{ lane }} is not null and occupancy_{{ lane }} is not null) as sample_ct_{{ lane }},
        {% endfor %}
    {% for lane in range(1, n_lanes+1) %}
        sum(flow_{{ lane }} * speed_{{ lane }})
        / nullifzero(sum(flow_{{ lane }})) as speed_weighted_{{ lane }}        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    from raw
    group by id, sample_date, sample_timestamp_trunc, district
),

{% for lane in range(1, n_lanes+1) %}
    agg_{{ lane }} as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_{{ lane }} as sample_ct,
            {{ lane }} as lane,
            flow_{{ lane }} as volume_sum,
            occupancy_{{ lane }} as occupancy_avg,
            speed_weighted_{{ lane }} as speed_weighted
        from agg
    ),
{% endfor %}

agg_unioned as (
    {% for lane in range(1, n_lanes+1) %}
        select * from agg_{{ lane }}
        {{ "union all" if not loop.last }}
    {% endfor %}
)

select *

from agg_unioned

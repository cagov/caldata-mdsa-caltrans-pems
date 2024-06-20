{{ config(
    materialized="incremental",
    cluster_by="sample_date",
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}
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
    {% if is_incremental() %}
        -- Look back two days to account for any late-arriving data
        where
            sample_date > (
                select
                    dateadd(
                        day,
                        {{ var("incremental_model_look_back") }},
                        max(sample_date)
                    )
                from {{ this }}
            )
            {% if target.name != 'prd' %}
                and sample_date
                >= dateadd(
                    day,
                    {{ var("dev_model_look_back") }},
                    current_date()
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where sample_date >= dateadd(day, {{ var("dev_model_look_back") }}, current_date())
    {% endif %}    
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

{% for lane in range(1, n_lanes+1) %}
    agg_{{ lane }} as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            {{ lane }} as lane,
            flow_{{ lane }} as flow,
            occupancy_{{ lane }} as occupancy,
            speed_{{ lane }} as speed
        from agg
    ),
{% endfor %}

agg_unioned as (
    {% for lane in range(1, n_lanes+1) %}
        select * from agg_{{ lane }}
        {{ "union all" if not loop.last }}
    {% endfor %}
)

select * from agg_unioned

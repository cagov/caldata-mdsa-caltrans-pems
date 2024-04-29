{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with

five_minute_agg as (
    select * from {{ ref ('int_clearinghouse__five_minute_station_agg') }}
    {% if is_incremental() %}
        -- Look back to account for any late-arriving data
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
                and sample_date >= (
                    dateadd(
                        day,
                        {{ var("dev_model_look_back") }},
                        current_date()
                    )
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where sample_date >= dateadd(day, {{ var("dev_model_look_back") }}, current_date())
    {% endif %}
),

five_minute_agg_with_station_meta as (
    select
        fma.*,
        sm.length,
        sm.type,
        sm._valid_from as station_valid_from,
        sm._valid_to as station_valid_to
    from five_minute_agg as fma
    inner join {{ ref ('int_clearinghouse__station_meta') }} as sm
        on
            fma.id = sm.id
            and fma.sample_date >= sm._valid_from
            and
            (
                fma.sample_date < sm._valid_to
                or sm._valid_to is null
            )
),

vmt_vht_metrics as (
    select
        *,
        volume * length as vmt, --vehicle-miles/5-min
        volume * length / nullifzero(speed) as vht, --vehicle-hours/5-min
        vmt / nullifzero(vht) as q_value,
        60 / nullifzero(q_value) as tti
    from five_minute_agg_with_station_meta
),

delay_metrics as (
    select
        vvm.*,
        /*  The formula for delay is: F * (L/V - L/V_t). F = flow (volume),
        L = length of the segment, V = current speed, and V_t = threshold speed. */
        {% for value in var("V_t") %}
            greatest(vvm.volume * (vvm.length / nullifzero(vvm.speed)) - (vvm.length / {{ value }})) as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}

    from vmt_vht_metrics as vvm
)

select * from delay_metrics

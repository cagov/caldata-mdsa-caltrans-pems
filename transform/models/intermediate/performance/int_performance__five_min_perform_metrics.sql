{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

{% set V_t_list = var('V_t') %}

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
            sm.id = fma.id
            and sm._valid_from <= fma.sample_date
            and
            (
                sm._valid_to > fma.sample_date
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
        *,

        /* The Delay performance metric is the amount of additional time spent
        by the vehicles on a section of road due to congestion. This is the
        difference between the travel time at a non-congestion speed and the
        current speed. The congestion, or threshold, speed is usually 35MPH but
        in PeMS we compute the delay for a number of different thresholds so that
        we can accommodate different definitions of delay. The threshold values
        used are 35, 40, 45, 50, 55, and 60 MPH. The formula is: D = F * (TT - TT_t) = F * (L/V - L/V_t).
        D = delay (can never be negative)
        TT = the travel time (unclear how this is calculated or where it comes from)
        TT_t = the travel time at the threshold speed (unclear how this is calculated or where it comes from)
        F = the flow
        L = the length of the segment
        V = the current speed
        V_t = the threshold speed */

        flow * () as delay_one,
        flow * (length / speed - length /{{ var("V_t") }}) as delay_two


    from vmt_vht_metrics
    where v_t in (
        {% for code in V_t_list %}
            {% if not loop.last %}
                {{ code }},
            {% else %}
                {{ code }}
            {% endif %}
        {% endfor %}
    )
)

select * from delay_metrics

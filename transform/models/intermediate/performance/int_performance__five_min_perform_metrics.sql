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
        --vehicle-miles/5-min
        volume_sum * length as vmt,
        --vehicle-hours/5-min 
        volume_sum * length / nullifzero(weighted_speed) as observed_vht,
        volume_sum * length / nullifzero(imputed_speed) as imputed_vht,
        --vehicle-hours/5-min 
        vmt / nullifzero(observed_vht) as observed_q_value,
        vmt / nullifzero(imputed_vht) as imputed_q_value,
        -- travel time
        60 / nullifzero(observed_q_value) as observed_tti,
        60 / nullifzero(imputed_q_value) as imputed_tti
    from five_minute_agg_with_station_meta
)

select * from vmt_vht_metrics
,
{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["id", "lane", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with
five_minute_agg as (
    select * from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

five_minute_agg_with_station_meta as (
    select
        fma.*,
        sm.length,
        sm.type,
        sm._valid_from as station_valid_from,
        sm._valid_to as station_valid_to
    from five_minute_agg as fma
    inner join {{ ref('int_clearinghouse__station_meta') }} as sm
        on
            fma.id = sm.id
            and fma.sample_date >= sm._valid_from
            and
            (
                fma.sample_date < sm._valid_to
                or sm._valid_to is null
            )
),

aggregated_speed as (
    select
        *,
        --A preliminary speed calcuation was developed on 3/22/24
        --using a vehicle effective length of 22 feet
        --(16 ft vehicle + 6 ft detector zone) feet and using
        --a conversion to get miles per hour (5280 ft / mile and 12
        --5-minute intervals in an hour).
        --The following code may be used if we want to use speed from raw data
        --coalesce(speed_raw, ((volume * 22) / nullifzero(occupancy)
        --* (1 / 5280) * 12))
        --impute five minutes missing speed
        coalesce(speed_weighted, (volume_sum * 22) / nullifzero(occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins,
        -- create a boolean function to track wheather speed is imputed or not
        coalesce(speed_five_mins != speed_weighted or (speed_five_mins is not null and speed_weighted is null), false)
        -- coalesce(speed_weighted is null, false)
            as is_speed_calculated
    from five_minute_agg_with_station_meta
),

vmt_vht_metrics as (
    select
        *,
        --vehicle-miles/5-min
        volume_sum * length as vmt,
        --vehicle-hours/5-min
        volume_sum * length / nullifzero(speed_five_mins) as vht,
        --q is in miles per hour for single station
        vmt / nullifzero(vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti
    from aggregated_speed
),

delay_metrics as (
    select
        vvm.*,
        /*  The formula for delay is: F * (L/V - L/V_t). F = flow (volume),
        L = length of the segment, V = current speed, and V_t = threshold speed. */
        {% for value in var("V_t") %}
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / {{ value }})), 0)
                as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}

    from vmt_vht_metrics as vvm
),

productivity_metrics as (
    select
        dm.*,
        /*
        The formula for Productivity is: Length * (1 - (actual flow / flow capacity))
        */
        {% for value in var("V_t") %}
            case
                when dm.speed_five_mins >= {{ value }}
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}

    from delay_metrics as dm
    inner join {{ ref("int_performance__max_capacity") }} as mc
        on dm.id = mc.id and dm.lane = mc.lane
)

select * from productivity_metrics

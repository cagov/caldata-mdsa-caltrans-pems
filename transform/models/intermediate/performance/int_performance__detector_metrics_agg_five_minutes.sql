{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    cluster_by=["sample_date"],
    event_time="sample_date",
    snowflake_warehouse = get_snowflake_refresh_warehouse()
) }}

with
five_minute_agg as (
    select
        station_id,
        lane,
        detector_id,
        sample_date,
        sample_timestamp,
        district,
        county,
        city,
        freeway,
        direction,
        length,
        sample_ct,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        station_type,
        absolute_postmile,
        volume_imputation_method,
        speed_imputation_method,
        occupancy_imputation_method,
        station_valid_from,
        station_valid_to
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
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
    from five_minute_agg
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
        on dm.detector_id = mc.detector_id
)

select * from productivity_metrics

{{ config(
    materialized="incremental",
    unique_key=['id','sample_date', 'sample_hour'],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}
-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (
    select
        *,
        date_trunc('hour', sample_timestamp) as sample_timestamp_trunc
    from {{ ref('int_performance__detector_metrics_agg_five_minutes') }}
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
    {% endif %}
),

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_station_temporal_metrics as (
    select
        id,
        sample_date,
        length,
        district,
        type,
        sample_timestamp_trunc as sample_hour,
        sum(volume_sum) as hourly_volume,
        avg(occupancy_avg) as hourly_occupancy,
        sum(volume_sum * speed_five_mins) / nullifzero(sum(volume_sum)) as hourly_speed,
        sum(vmt) as hourly_vmt,
        sum(vht) as hourly_vht,
        hourly_vmt / nullifzero(hourly_vht) as hourly_q_value,
        -- travel time
        60 / nullifzero(hourly_q_value) as hourly_tti,
        {% for value in var("V_t") %}
            greatest(
                hourly_volume * ((length / nullifzero(hourly_speed)) - (length / {{ value }})), 0
            )
                as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %},
        {% for value in var("V_t") %}
            sum(lost_productivity_{{ value }}_mph)
                as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from station_five_mins_data
    group by id, sample_date, sample_hour, district, type, length
)

select * from hourly_station_temporal_metrics

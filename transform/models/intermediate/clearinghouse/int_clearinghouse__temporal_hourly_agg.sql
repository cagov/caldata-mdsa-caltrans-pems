{{ config(
    materialized="incremental",
    unique_key=['id','sample_date', 'sample_hour', 'lane'],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

-- read the volume, occupancy and speed five minutes data
with station_five_mins_data as (
    select
        *,
        date_trunc('hour', sample_timestamp) as sample_timestamp_trunc
    from {{ ref('int_performance__five_min_perform_metrics') }}

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

five_minute_agg_with_station_meta as (
    select
        fma.*,
        sm.city,
        sm.county,
        sm.freeway,
        sm.direction,
        sm._valid_from as station_valid_from,
        sm._valid_to as station_valid_to
    from station_five_mins_data as fma
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

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_spatial_temporal_metrics as (
    select
        id,
        lane,
        sample_date,
        city,
        county,
        district,
        type,
        direction,
        freeway,
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
            sum(delay_{{ value }}_mph)
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
    from five_minute_agg_with_station_meta
    group by id, sample_date, sample_hour, lane, district, county, city, freeway, direction, type
)

select * from hourly_spatial_temporal_metrics

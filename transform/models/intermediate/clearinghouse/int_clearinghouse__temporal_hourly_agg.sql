{{ config(
    materialized="incremental",
    unique_key=['id', 'sample_hour', 'lane'],
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

-- now aggregate five mins volume, occupancy and speed to hourly
hourly_temporal_metrics as (
    select
        id,
        lane,
        sample_timestamp_trunc as sample_hour,
        sum(volume_sum) as volume_sum,
        avg(occupancy_avg) as occupancy_avg,
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
    from station_five_mins_data
    group by id, sample_hour, lane
),

-- read spatial characteristics
hourly_spatial_temporal_metrics as (
    select
        hourly_temporal_metrics.*,
        station_meta_data.city,
        station_meta_data.county,
        station_meta_data.district,
        station_meta_data.type
    from {{ ref('int_clearinghouse__most_recent_station_meta') }} as station_meta_data
    inner join hourly_temporal_metrics
        on
            station_meta_data.id = hourly_temporal_metrics.id
            and station_meta_data.lanes = hourly_temporal_metrics.lane
)

select * from hourly_spatial_temporal_metrics

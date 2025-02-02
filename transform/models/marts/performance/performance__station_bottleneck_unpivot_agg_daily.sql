{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_date)) || '/month=' || to_varchar(date_part(month, sample_date)))",
) }}

with daily_bottleneck_delay as (
    select * from {{ ref('int_performance__bottleneck_delay_metrics_agg_daily') }}
),

unpivot_delay as (
    select
        station_id,
        sample_date,
        time_shift,
        station_type,
        district,
        freeway,
        direction,
        absolute_postmile,
        county,
        target_speed,
        sum(coalesce(delay, 0)) as delay
    from (
        {% for value in var("V_t") %}
            select
                station_id,
                sample_date,
                time_shift,
                station_type,
                district,
                freeway,
                direction,
                absolute_postmile,
                county,
                '{{ value }}' as target_speed,
                daily_time_shift_spatial_delay_{{ value }}_mph as delay
            from
                daily_bottleneck_delay
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_metrics
    group by
        station_id,
        sample_date,
        time_shift,
        station_type,
        district,
        freeway,
        direction,
        absolute_postmile,
        county,
        target_speed
),

bottleneck_delay_with_county as (
    {{ get_county_name('unpivot_delay') }}
),

geo as (
    select
        station_id,
        latitude,
        longitude,
        concat(longitude, ',', latitude) as location
    from {{ ref('geo__current_stations') }}
),

bottleneck_delay_county_geo as (
    select
        bottleneck_delay_with_county.*,
        geo.latitude,
        geo.longitude,
        geo.location
    from
        bottleneck_delay_with_county
    inner join
        geo
        on bottleneck_delay_with_county.station_id = geo.station_id
)

select * from bottleneck_delay_county_geo

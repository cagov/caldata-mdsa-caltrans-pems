{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_month)) || '/month=' || to_varchar(date_part(month, sample_month)))",
) }}

with detector_daily_status as (
    select
        *,
        DATE_TRUNC(month, sample_date) as sample_month
    from {{ ref('diagnostics__detector_daily_by_station') }}
),

recent_data as (
    select
        district,
        station_id,
        station_type,
        sample_month,
        county,
        city,
        freeway,
        direction,
        latitude,
        longitude,
        length,
        state_postmile,
        absolute_postmile,
        ROW_NUMBER() over (
            partition by
                district, station_id, station_type, sample_month, county, city, freeway, direction, latitude, longitude
            order by sample_date desc
        ) as rn
    from
        detector_daily_status
),

detector_monthly_status_by_station as (
    select
        district,
        station_id,
        station_type,
        sample_month,
        county,
        city,
        freeway,
        direction,
        latitude,
        longitude,
        SUM(detector_count) as monthly_detector_count,
        ROUND(SUM(average_sample_count)) as monthly_sample_count,
        SUM(good_detector_count) as monthly_good_detector_count,
        SUM(good_count) as good_count,
        SUM(down_or_no_data_count) as down_or_no_data_count,
        SUM(insufficient_data_count) as insufficient_data_count,
        SUM(card_off_count) as card_off_count,
        SUM(high_val_count) as high_val_count,
        SUM(intermittent_count) as intermittent_count,
        SUM(constant_count) as constant_count
    from
        detector_daily_status
    group by
        district,
        station_id,
        station_type,
        sample_month,
        county,
        city,
        freeway,
        direction,
        latitude,
        longitude
),

monthly_station_health as (
    select
        dms.*,
        recent.length,
        recent.state_postmile,
        recent.absolute_postmile
    from
        detector_monthly_status_by_station as dms
    left join
        recent_data
            as recent
        on
            dms.district = recent.district
            and dms.station_id = recent.station_id
            and dms.station_type = recent.station_type
            and dms.sample_month = recent.sample_month
            and dms.county = recent.county
            and dms.city = recent.city
            and dms.freeway = recent.freeway
            and dms.direction = recent.direction
            and dms.latitude = recent.latitude
            and dms.longitude = recent.longitude
            and recent.rn = 1
)

select * from monthly_station_health

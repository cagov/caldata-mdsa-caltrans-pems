{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_month)) || '/month=' || to_varchar(date_part(month, sample_month)))",
) }}

with

detector_daily_status as (
    select
        *,
        date_trunc(month, sample_date) as sample_month
    from {{ ref('diagnostics__detector_daily_by_station') }}
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
        sum(detector_count) as monthly_detector_count,
        round(sum(average_sample_count)) as monthly_sample_count,
        sum(good_detector_count) as monthly_good_detector_count,
        sum(good) as good_count,
        sum(down_or_no_data) as down_or_no_data_count,
        sum(insufficient_data) as insufficient_data_count,
        sum(card_off) as card_off_count,
        sum(high_val) as high_val_count,
        sum(intermittent) as intermittent_count,
        sum(constant) as constant_count
    from detector_daily_status
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
)

select *
from
    detector_monthly_status_by_station

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
        state_postmile,
        absolute_postmile,
        county,
        city,
        freeway,
        direction,
        physical_lanes,
        length,
        latitude,
        longitude,
        round(avg(detector_count)) as average_monthly_detector_count,
        round(sum(average_sample_count)) as monthly_sample_count,
        round(avg(good_detector_count)) as monthly_average_good_detector_count,
        round(avg(good)) as good,
        round(avg(down_or_no_data)) as down_or_no_data,
        round(avg(insufficient_data)) as insufficient_data,
        round(avg(card_off)) as card_off,
        round(avg(high_val)) as high_val,
        round(avg(intermittent)) as intermittent,
        round(avg(constant)) as constant
    from detector_daily_status
    group by
        district,
        station_id,
        station_type,
        sample_month,
        state_postmile,
        absolute_postmile,
        county,
        city,
        freeway,
        physical_lanes,
        direction,
        length,
        latitude,
        longitude
)

select *
from
    detector_monthly_status_by_station

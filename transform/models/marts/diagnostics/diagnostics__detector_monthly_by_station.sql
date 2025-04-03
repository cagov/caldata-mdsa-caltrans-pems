{{ config(
    materialized="table",
    unload_partitioning="('year=' || to_varchar(date_part(year, sample_month)) || '/month=' || to_varchar(date_part(month, sample_month)))",
) }}

with detector_daily_status as (
    select
        *,
        DATE_TRUNC(month, sample_date) as sample_month,
        ROW_NUMBER() over (partition by sample_month, station_id order by sample_date desc) as rn
    from {{ ref('diagnostics__detector_daily_by_station') }}
),

detector_monthly_status_by_station as (
    select
        sample_month,
        station_id,
        county,
        county_name,
        county_abb,
        MAX(case when rn = 1 then district end) as district,
        MAX(case when rn = 1 then state_postmile end) as state_postmile,
        MAX(case when rn = 1 then absolute_postmile end) as absolute_postmile,
        MAX(case when rn = 1 then latitude end) as latitude,
        MAX(case when rn = 1 then longitude end) as longitude,
        MAX(case when rn = 1 then station_type end) as station_type,
        MAX(case when rn = 1 then city end) as city,
        MAX(case when rn = 1 then freeway end) as freeway,
        MAX(case when rn = 1 then direction end) as direction,
        SUM(detector_count) as monthly_detector_count,
        SUM(good_detector_count) as monthly_good_detector_count,
        SUM(bad_detector_count) as monthly_bad_detector_count,
        SUM(down_or_no_data_count) as down_or_no_data_count,
        SUM(insufficient_data_count) as insufficient_data_count,
        SUM(card_off_count) as card_off_count,
        SUM(high_val_count) as high_val_count,
        SUM(intermittent_count) as intermittent_count,
        SUM(constant_count) as constant_count
    from
        detector_daily_status
    group by
        station_id,
        sample_month,
        county
)

select * from detector_monthly_status_by_station

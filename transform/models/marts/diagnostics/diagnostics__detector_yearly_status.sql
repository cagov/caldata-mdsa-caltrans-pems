{{ config(
    materialized="table"
) }}

with meta_data as (
    select
        *,
        case
            when status = 'Good' then 'Good'
            else 'Bad'
        end as detector_status
    from {{ ref('diagnostics__detector_daily_detail') }}
    where
        sample_date >= '2023-01-01'
        and sample_date < '2024-01-01' and station_type in ('HV', 'ML')
),

detector_status as (
    select
        station_id,
        lane,
        station_type,
        COUNT(case when detector_status = 'Good' then 1 end) as good_detector_count,
        COUNT(case when detector_status = 'Bad' then 1 end) as bad_detector_count,
        COUNT(detector_status) as total_detector_count
    from meta_data
    group by station_id, station_type, lane
),

good_detector_pct as (
    select
        *,
        (good_detector_count * 1.0 / NULLIF(total_detector_count, 0)) * 100 as pct_good_detector
    from detector_status
)

select *
from good_detector_pct

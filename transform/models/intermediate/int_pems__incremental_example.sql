{{
config(
        materialized='incremental',
        unique_key=['id', 'sample_date']
)
}}

with station_raw as (
    select *
    from {{ ref('stg_clearinghouse__station_raw') }}
    {% if is_incremental() %}
        where
            sample_date >= dateadd(day, -7, (select max(sample_date) from {{ this }}))
    {% endif %}
)

select
    id,
    sample_date,
    count(flow_1) as n_samples_lane_1,
    count(flow_2) as n_samples_lane_2,
    count(flow_3) as n_samples_lane_3,
    count(flow_4) as n_samples_lane_4,
    count(flow_5) as n_samples_lane_5,
    count(flow_6) as n_samples_lane_6,
    count(flow_7) as n_samples_lane_7,
    count(flow_8) as n_samples_lane_8
from station_raw
group by id, sample_date
having sample_date >= '2023-12-01'

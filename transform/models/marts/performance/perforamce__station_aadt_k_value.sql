with aadt as (
    select *
    from {{ ref('int_performance__station_aadt_with_K_value') }}
    where extract(year from sample_year) >= extract(year from current_date) - 1
)

select *
from aadt

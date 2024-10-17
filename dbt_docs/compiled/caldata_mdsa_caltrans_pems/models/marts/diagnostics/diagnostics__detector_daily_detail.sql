

with

detector_status as (
    select * from ANALYTICS_PRD.diagnostics.int_diagnostics__detector_status
    where sample_date is not null and lane is not null
)

select * from detector_status
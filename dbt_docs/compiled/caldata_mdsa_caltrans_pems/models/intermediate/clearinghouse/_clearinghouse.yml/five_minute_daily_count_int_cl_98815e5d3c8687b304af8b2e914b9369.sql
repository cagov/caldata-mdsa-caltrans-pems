
with
validation_errors as (
    select detector_id,sample_date
    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__detector_agg_five_minutes_with_missing_rows
    group by detector_id,sample_date
    having count(*) != 288
)

select * from validation_errors


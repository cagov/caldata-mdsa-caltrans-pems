{% test five_minute_daily_count(model, group_by_columns) %}
with
validation_errors as (
    select {{ group_by_columns|join(',') }}
    from {{ model }}
    group by {{ group_by_columns|join(',') }}
    having count(*) != 288
)

select * from validation_errors

{% endtest %}
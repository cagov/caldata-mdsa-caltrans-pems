{% test not_empty(model) %}

with validation as (
   select count(0) as num_rows
   from {{ model }}
),

validation_errors as (
   select num_rows
   from validation
   where num_rows = 0
)

select * from validation_errors

{% endtest %}

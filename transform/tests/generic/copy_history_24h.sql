{% test copy_history_24h(model) %}

/* This grabs the model's available copy history (past 14 days) from Snowflake
see: https://docs.snowflake.com/en/sql-reference/functions/copy_history
*/
with validation as (
   select LAST_LOAD_TIME
   from table(
    INFORMATION_SCHEMA.COPY_HISTORY(
        table_name => '{{ model }}',
        start_time=> DATEADD(days, -14, CURRENT_TIMESTAMP())
        )
    )
),

-- This checks if the last recorded load time was more than 24 hours ago
validation_errors as (
   select LAST_LOAD_TIME
   from validation
   where LAST_LOAD_TIME < DATEADD(days, -1, CURRENT_TIMESTAMP())
)

select * from validation_errors

{% endtest %}
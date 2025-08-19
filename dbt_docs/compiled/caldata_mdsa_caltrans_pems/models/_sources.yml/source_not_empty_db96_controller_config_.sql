

with validation as (
   select count(0) as num_rows
   from RAW_PRD.db96.controller_config
),

validation_errors as (
   select num_rows
   from validation
   where num_rows = 0
)

select * from validation_errors


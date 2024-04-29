/* Per the PEMS Performance Measrues documentation (https://pems.dot.ca.gov/?dnode=Help&content=help_calc#perf)
Delay can never be negative. Instead of coercing Delay to be positive or only selecting instances where Delay
is positive we have written a test to cacth these instances. */
select delay
from {{ ref('int_performance__five_min_perform_metrics' ) }}
where delay < 0

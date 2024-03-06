/*
This SQL file assigns which detector threshold values will be used
for a station based on information from the station metadata.
*/
select
    ddsa.*,
    dtv.dt_name,
    dtv.dt_value

from {{ ref('stg_pems__set_assignment') }} as ddsa
inner join {{ ref('diagnostic_threshold_values') }} as dtv
    on
        ddsa.det_diag_set_id = dtv.dt_set_id
        and ddsa.det_diag_method_id = dtv.dt_method

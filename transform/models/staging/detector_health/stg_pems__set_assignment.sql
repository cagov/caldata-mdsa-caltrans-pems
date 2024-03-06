/*
This SQL file assigns which sets of calculations will be used for a station
based on information in from the station metadata
The DET_DIAG_SET_ID varialbe assigns 1 of 5 values for detector diagnostic
evaluations. The DET_DIAG_METHOD_ID variable assigns 1 of 2 values for detector
diagnostic evaluations.
*/
select
    meta_date,
    id as station_id,
    district,
    type,
    case
        --when LIKE(UPPER(THRESHOLD_SET), "LOW%") then "Low_Volume" currently in district config file
        --when LIKE(UPPER(THRESHOLD_SET), "RURAL%") then "Rural" need definition from Iteris
        when district = 11 then 'Urban_D11'
        when district = 6 then 'D6_Ramps'
        else 'Urban'
    end as det_diag_set_id,
    case
        when type = 'OR' then 'ramp'
        else 'mainline'
    end as det_diag_method_id

from {{ ref('stg_clearinghouse__station_meta') }}
where
    DATE(meta_date) = DATEADD(year, -1, CURRENT_DATE())

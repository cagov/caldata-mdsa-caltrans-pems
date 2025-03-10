with

station_diagnostic_set_assign as (
    /*
    This SQL file assigns which sets of calculations will be used for
    a station based on information in from the station metadata
    The station_DIAGNOSTIC_SET_ID variable assigns 1 of 5 values for station
    diagnostic evaluations. The station_DIAGNOSTIC_METHOD_ID variable assigns
    1 of 2 values for station diagnostic evaluations.
    */
    select
        active_date,
        station_id,
        district,
        station_type,
        dt_set_id,
        _valid_from as station_valid_from,
        _valid_to as station_valid_to,
        case
            when UPPER(dt_set_id) like 'LOW%' then 'Low_Volume'
            when UPPER(dt_set_id) like 'RURAL%' then 'Rural'
            when UPPER(dt_set_id) like 'URBAN_D11%' then 'Urban_D11'
            when UPPER(dt_set_id) like 'D6_RAMPS%' then 'D6_Ramps'
            else 'Urban'
        end as station_diagnostic_set_id,
        case
            when station_type in ('ML', 'HV') then 'mainline'
            else 'ramp'
        end as station_diagnostic_method_id

    from {{ ref ('int_vds__active_stations') }}
),

diagnostic_threshold_values as (
    -- Pivot the data in the diagnostic_threshold_value seed file so
    -- subsequent joins create wide instead of long tables
    select *
    from {{ ref('diagnostic_threshold_values') }}
    pivot (AVG(dt_value) for dt_name in (
        'high_occ',
        'high_flow',
        'high_occ_pct',
        'zero_occ_pct',
        'flow_occ_pct',
        'occ_flow_pct',
        'repeat_occ',
        'high_flow_pct',
        'zero_flow_pct'
    ))
        as p (
            dt_set_id,
            dt_method,
            high_occupancy,
            high_flow,
            high_occupancy_percent,
            zero_occupancy_percent,
            flow_occupancy_percent,
            occupancy_flow_percent,
            repeat_occupancy,
            high_flow_percent,
            zero_flow_percent
        )
),

station_diagnostic_threshold_values as (
    /*
    This SQL file assigns which station threshold values will be used
    for a station based on information from the station metadata.
    */
    select
        station_diagnostic_set_assign.*,
        diagnostic_threshold_values.* exclude (dt_set_id, dt_method)
    from station_diagnostic_set_assign
    inner join diagnostic_threshold_values
        on
            station_diagnostic_set_assign.station_diagnostic_set_id = diagnostic_threshold_values.dt_set_id
            and station_diagnostic_set_assign.station_diagnostic_method_id = diagnostic_threshold_values.dt_method
)

select * from station_diagnostic_threshold_values

{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="sample_date",
    full_refresh=false,
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

with
source as (
    select * from {{ ref('int_diagnostics__samples_per_detector') }}
),

detector_meta as (
    select * from {{ ref('int_vds__detector_config') }}
),

set_assgnmt as (
    select * from {{ ref('int_diagnostics__det_diag_set_assignment') }}
    -- TODO: maybe add an event_time config here?
    where active_date between (select min(sample_date) from source) and (select max(sample_date) from source)
),

assignment_with_meta as (
    select
        set_assgnmt.*,
        dm.detector_id,
        dm.detector_type,
        dm.lane,
        dm.state_postmile,
        dm.absolute_postmile,
        dm.latitude,
        dm.longitude,
        dm.physical_lanes,
        dm.county,
        dm.city,
        dm.freeway,
        dm.direction,
        dm.length
    from set_assgnmt
    inner join detector_meta as dm
        on
            set_assgnmt.station_id = dm.station_id
            and {{ get_scd_2_data('set_assgnmt.active_date','dm._valid_from','dm._valid_to') }}
),

detector_status as (
    select
        awm.active_date,
        awm.station_id,
        awm.district,
        awm.station_type,
        awm.station_diagnostic_method_id,
        awm.active_date as sample_date,
        awm.detector_id,
        awm.detector_type,
        awm.lane,
        awm.state_postmile,
        awm.absolute_postmile,
        awm.latitude,
        awm.longitude,
        awm.physical_lanes,
        awm.county,
        awm.city,
        awm.freeway,
        awm.direction,
        awm.length,
        sps.* exclude (district, station_id, lane, detector_id, sample_date),
        nds.district_feed_working,
        nds.line_num_working,
        nds.controller_feed_working,
        nds.station_feed_working,
        nds.detector_feed_working,
        co.min_occupancy_delta,
        case
            when nds.district_feed_working = 'No' then 'District Feed Down'
            when nds.line_num_working = 'No' then 'Line Down'
            when nds.controller_feed_working = 'No' then 'Controller Down'
            when nds.detector_feed_working = 'No' then 'No Data'
            when sps.sample_ct = 0 or sps.sample_ct is null
                then 'Down/No Data'
            /* # of samples < 60% of the max collected during the test period
            max value: 2 samples per min * 60 mins/hr * 17 hrs in a day == 1224
            btwn 1 and 1224 is too few samples */
            when sps.sample_ct between 1 and (0.6 * ({{ var("detector_status_max_sample_value") }}))
                then 'Insufficient Data'
            when
                awm.station_diagnostic_method_id = 'ramp'
                and
                (sps.zero_vol_ct / sps.sample_ct)
                >= (awm.zero_flow_percent / 100)
                then 'Card Off'
            when
                awm.station_diagnostic_method_id = 'mainline'
                and
                (sps.zero_occ_ct / sps.sample_ct)
                >= (awm.zero_occupancy_percent / 100)
                then 'Card Off'
            when
                awm.station_diagnostic_method_id = 'ramp'
                and
                (sps.high_volume_ct / sps.sample_ct)
                >= (awm.high_flow_percent / 100)
                then 'High Val'
            when
                awm.station_diagnostic_method_id = 'mainline'
                and
                (sps.high_occupancy_ct / sps.sample_ct)
                >= (awm.high_occupancy_percent / 100)
                then 'High Val'
            when
                awm.station_diagnostic_method_id = 'mainline'
                and
                (sps.zero_vol_pos_occ_ct / sps.sample_ct)
                >= (awm.flow_occupancy_percent / 100)
                then 'Intermittent'
            when
                awm.station_diagnostic_method_id = 'mainline'
                and
                (sps.zero_occ_pos_vol_ct / sps.sample_ct)
                >= (awm.occupancy_flow_percent / 100)
                then 'Intermittent'
            when
            -- the float value can not compare with 0, we set a small threshold to replace with it
                coalesce(co.min_occupancy_delta < 0.00001, false)
                and awm.station_diagnostic_method_id = 'mainline'
                then 'Constant'
            --Feed unstable case needed
            else 'Good'
        end as status

    from assignment_with_meta as awm

    left join source as sps
        on
            awm.detector_id = sps.detector_id
            -- and awm.lane = sps.lane
            and awm.active_date = sps.sample_date

    left join {{ ref('int_diagnostics__constant_occupancy') }} as co
        on
            awm.detector_id = co.detector_id
            and awm.active_date = co.sample_date

    left join {{ ref('int_diagnostics__no_data_status') }} as nds
        on
            awm.active_date = nds.active_date
            and awm.detector_id = nds.detector_id
)

select * from detector_status

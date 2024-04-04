{{ config(
    materialized="incremental",
    snowflake_warehouse="transforming_xl_dev"
) }}

with

detector_status as (
    select
        sps.*,
        case
            when sps.sample_ct = 0 or sps.sample_ct is null
                then 'Down/No Data'
            -- # of samples < 60% of the max collected samples during the test period
            -- max value: 2 samples per minute times 60 mins/hr times 17 hours in a day which == 1224
            -- btwn 1 and 1224 is too few samples
            when sps.sample_ct between 1 and (0.6 * (2 * 60 * 17))
                then 'Insufficient Data'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.zero_vol_ct / (2 * 60 * 17) > (set_assgnmt.zero_flow_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_ct / (2 * 60 * 17) > (set_assgnmt.zero_occupancy_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.high_volume_ct / (2 * 60 * 17) > (set_assgnmt.high_flow_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.high_occupancy_ct / (2 * 60 * 17) > (set_assgnmt.high_occupancy_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_vol_pos_occ_ct / (2 * 60 * 17) > (set_assgnmt.flow_occupancy_percent / 100)
                then 'Intermittent'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_pos_vol_ct / (2 * 60 * 17) > (set_assgnmt.occupancy_flow_percent / 100)
                then 'Intermittent'
            --constant occupancy case needed
            --Feed unstable case needed
            else 'Good'
        end as status
    from {{ ref('int_pems__det_diag_set_assignment') }} as set_assgnmt
    inner join {{ ref('int_pems__diagnostic_samples_per_station') }} as sps
        on
            sps.station_id = set_assgnmt.station_id
            and sps.lane_num = set_assgnmt.lane_num
)

select * from detector_status

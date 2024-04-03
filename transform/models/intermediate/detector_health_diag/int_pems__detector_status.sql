{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['station_id', 'sample_date', 'lane'],
    snowflake_warehouse="transforming_xl_dev"
) }}

with
det_diag_sample_count as (
    select * from {{ ref ('int_pems__diagnostic_samples_per_station') }}
    where sample_date <= CURRENT_DATE
        --TO_TIME(sample_timestamp) >= '05:00:00' and TO_TIME(sample_timestamp) <= '21:59:59'
        {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select DATEADD(day, -2, MAX(sample_date)) from {{ this }}
            )
        {% endif %}
        {% if target.name != 'prd' %}
            and sample_date >= DATEADD('day', -2, CURRENT_DATE())
        {% endif %}
),

detector_status as (
    select
        ddsc.*,
        case
            when ddsc.sample_ct = 0 or ddsc.sample_ct is null then 'Down/No Data'
            -- # of samples < 60% of the max collected samples during the test period
            -- max value: 2 samples per minute times 60 mins/hr times 17 hours in a day which == 1224
            -- btwn 1 and 1224 is too few samples
            when ddsc.sample_ct between 1 and (0.6 * (2 * 60 * 17)) then 'Insufficient Data'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and ddsc.zero_vol_ct / (2 * 60 * 17) > (set_assgnmt.zero_flow_percent / 100) then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and ddsc.zero_occ_ct / (2 * 60 * 17) > (set_assgnmt.zero_occupancy_percent / 100) then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and ddsc.high_volume_ct / (2 * 60 * 17) > (set_assgnmt.high_flow_percent / 100) then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and ddsc.high_occupancy_ct / (2 * 60 * 17) > (set_assgnmt.high_occupancy_percent / 100) then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and ddsc.zero_vol_pos_occ_ct / (2 * 60 * 17) > (set_assgnmt.flow_occupancy_percent / 100) then 'Intermittent'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and ddsc.zero_occ_pos_vol_ct / (2 * 60 * 17) > (set_assgnmt.occupancy_flow_percent / 100) then 'Intermittent'
            --constant occupancy case needed
            --Feed unstable case needed
            else 'Good'
        end as status
    from {{ ref('int_pems__det_diag_set_assignment') }} as set_assgnmt
    inner join det_diag_sample_count as ddsc
        on ddsc.station_id = set_assgnmt.station_id
    --Look only at the previous day to determine a detectors status
    --for development we use 2 days but the final code should be updated
    --from -2 to -1 or use PREVIOUS_DAY in Snowflake
    where ddsc.sample_date = DATEADD('day', -2, CURRENT_DATE())
)

select * from detector_status

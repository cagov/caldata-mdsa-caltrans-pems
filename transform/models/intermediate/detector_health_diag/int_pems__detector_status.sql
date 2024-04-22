{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['id', 'lane', 'sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with

source as (
    select * from {{ ref('int_pems__diagnostic_samples_per_station') }}
    where
        TO_TIME(sample_timestamp) >= {{ var("day_start") }}
        and TO_TIME(sample_timestamp) <= {{ var("day_end") }}
        {% if is_incremental() %}
            -- Look back two days to account for any late-arriving data
            and sample_date > (
                select
                    DATEADD(day, {{ var("incremental_model_look_back") }}, MAX(sample_date))
                from {{ this }}
            )
        {% endif %}
        {% if target.name != 'prd' %}
            and sample_date
            >= DATEADD('day', {{ var("dev_model_look_back") }}, CURRENT_DATE())
        {% endif %}
),

detector_status as (
    select
        sps.*,
        co.min_occupancy_delta,
        case
            when sps.sample_ct = 0 or sps.sample_ct is null
                then 'Down/No Data'
            /* # of samples < 60% of the max collected during the test period
            max value: 2 samples per min * 60 mins/hr * 17 hrs in a day == 1224
            btwn 1 and 1224 is too few samples */
            when sps.sample_ct between 1 and (0.6 * ({{ var("detector_status_max_sample_value") }}))
                then 'Insufficient Data'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.zero_vol_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.zero_flow_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.zero_occupancy_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.high_volume_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.high_flow_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.high_occupancy_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.high_occupancy_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_vol_pos_occ_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.flow_occupancy_percent / 100)
                then 'Intermittent'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_pos_vol_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.occupancy_flow_percent / 100)
                then 'Intermittent'
            when COALESCE(co.min_occupancy_delta = 0, false)
                then 'Constant'
            --Feed unstable case needed
            else 'Good'
        end as status
    from {{ ref('int_pems__det_diag_set_assignment') }} as set_assgnmt
    left join source as sps
        on
            set_assgnmt.station_id = sps.station_id
            and set_assgnmt.station_valid_from <= sps.sample_date
            and
            (
                set_assgnmt.station_valid_to > sps.sample_date
                or set_assgnmt.station_valid_to is null
            )
    left join {{ ref('int_pems__constant_occupancy') }} as co
        on
            sps.station_id = co.id and sps.lane = co.lane and sps.sample_date = co.sample_date
)

select * from detector_status

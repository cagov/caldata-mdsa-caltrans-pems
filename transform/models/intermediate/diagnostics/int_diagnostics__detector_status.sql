{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['station_id', 'lane', 'sample_date'],
    on_schema_change='sync_all_columns',
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with

source as (
    select * from {{ ref('int_diagnostics__samples_per_detector') }}
    where {{ make_model_incremental('sample_date') }}
),

detector_meta as (
    select * from {{ ref('int_vds__detector_config') }}
),

district_feed_check as (
    select
        source.district,
        case
            when (count_if(source.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as district_feed_working
    from source
    inner join {{ ref('districts') }} as d
        on source.district = d.district_id
    group by source.district
),

detector_status as (
    select
        set_assgnmt.active_date,
        set_assgnmt.station_id,
        set_assgnmt.district,
        set_assgnmt.station_type,
        set_assgnmt.active_date as sample_date,
        dm.detector_id,
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
        dm.length,
        sps.* exclude (district, station_id, lane, detector_id, sample_date),
        dfc.district_feed_working,
        co.min_occupancy_delta,
        case
            when dfc.district_feed_working = 'No' then 'District Feed Down'
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
            when
                coalesce(co.min_occupancy_delta = 0, false)
                and set_assgnmt.station_diagnostic_method_id = 'mainline'
                then 'Constant'
            --Feed unstable case needed
            else 'Good'
        end as status

    from {{ ref('int_diagnostics__det_diag_set_assignment') }} as set_assgnmt
    inner join detector_meta as dm
        on
            set_assgnmt.station_id = dm.station_id
            and {{ get_scd_2_data('set_assgnmt.active_date','dm._valid_from','dm._valid_to') }}

    left join source as sps
        on
            set_assgnmt.station_id = sps.station_id
            and dm.lane = sps.lane
            and set_assgnmt.active_date = sps.sample_date

    left join {{ ref('int_diagnostics__constant_occupancy') }} as co
        on
            set_assgnmt.station_id = co.station_id
            and sps.lane = co.lane
            and set_assgnmt.active_date = co.sample_date

    left join district_feed_check as dfc
        on set_assgnmt.district = dfc.district
)

select * from detector_status

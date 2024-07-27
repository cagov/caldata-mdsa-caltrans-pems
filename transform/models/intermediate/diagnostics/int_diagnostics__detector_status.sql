{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['detector_id', 'sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with

source_samples as (
    select * from {{ ref('int_diagnostics__samples_per_detector') }}
    where {{ make_model_incremental('sample_date') }}
),

source_detectors as (
    select
        active_date,
        detector_id,
        station_id,
        lane,
        detector_type,
        station_type,
        district
    from {{ ref ('int_clearinghouse__active_detectors') }}
),

source_detector_samples as (
    select
        ss.* exclude (district, detector_id, station_id, lane),
        sd.*
    from source_detectors as sd
    left join source_samples as ss
        on
            sd.detector_id = ss.detector_id
            and sd.active_date = ss.sample_date
),

district_feed_check as (
    select
        source_samples.district,
        source_samples.sample_date,
        case
            when (count_if(source_samples.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as district_feed_working
    from source_samples
    inner join {{ ref('districts') }} as d
        on source_samples.district = d.district_id
    group by source_samples.district, source_samples.sample_date
),

detector_status as (
    select
        sds.* exclude (district, station_id, station_type),
        set_assgnmt.station_id,
        set_assgnmt.district,
        set_assgnmt.station_type,
        dfc.district_feed_working,
        co.min_occupancy_delta,
        case
            when dfc.district_feed_working = 'No' then 'District Feed Down'
            when sds.sample_ct = 0 or sds.sample_ct is null
                then 'Down/No Data'
            /* # of samples < 60% of the max collected during the test period
            max value: 2 samples per min * 60 mins/hr * 17 hrs in a day == 1224
            btwn 1 and 1224 is too few samples */
            when sds.sample_ct between 1 and (0.6 * ({{ var("detector_status_max_sample_value") }}))
                then 'Insufficient Data'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sds.zero_vol_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.zero_flow_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sds.zero_occ_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.zero_occupancy_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sds.high_volume_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.high_flow_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sds.high_occupancy_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.high_occupancy_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sds.zero_vol_pos_occ_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.flow_occupancy_percent / 100)
                then 'Intermittent'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sds.zero_occ_pos_vol_ct / ({{ var("detector_status_max_sample_value") }})
                > (set_assgnmt.occupancy_flow_percent / 100)
                then 'Intermittent'
            when coalesce(co.min_occupancy_delta = 0, false)
                then 'Constant'
            --Feed unstable case needed
            else 'Good'
        end as status

    from source_detector_samples as sds
    left join {{ ref('int_diagnostics__det_diag_set_assignment') }} as set_assgnmt
        on
            sds.station_id = set_assgnmt.station_id
            and sds.sample_date = set_assgnmt.active_date

    left join {{ ref('int_diagnostics__constant_occupancy') }} as co
        on
            sds.station_id = co.station_id and sds.lane = co.lane and sds.sample_date = co.sample_date
    left join district_feed_check as dfc
        on
            set_assgnmt.district = dfc.district
            and set_assgnmt.active_date = dfc.sample_date
)

select * from detector_status where sample_date is not null

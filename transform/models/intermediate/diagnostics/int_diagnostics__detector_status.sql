{{ config(
    materialized="incremental",
    cluster_by=['sample_date'],
    unique_key=['station_id', 'lane', 'sample_date'],
    snowflake_warehouse=get_snowflake_refresh_warehouse(small="XL")
) }}

with

source as (
    select * from {{ ref('int_diagnostics__samples_per_detector') }}
    where {{ make_model_incremental('sample_date') }}
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

dates_raw as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="current_date()"
        )
    }}
),

detector_metdata as (
    select
        vdc.station_id,
        vdc.detector_id,
        vdc.district,
        vdc.lane,
        vdc._valid_from,
        vdc._valid_to,
        dr.date_day
    from dates_raw as dr
    left join {{ ref('int_vds__detector_config') }} as vdc
        on dr.date_day between vdc._valid_from and coalesce(vdc._valid_to, current_date)
),


detector_status as (
    select
        set_assgnmt.active_date,
        set_assgnmt.station_id,
        set_assgnmt.district,
        set_assgnmt.type,
        sps.* exclude (district, station_id),
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
            when coalesce(co.min_occupancy_delta = 0, false)
                then 'Constant'
            --Feed unstable case needed
            else 'Good'
        end as status

    from {{ ref('int_diagnostics__det_diag_set_assignment') }} as set_assgnmt
    left join source as sps
        on
            set_assgnmt.station_id = sps.station_id
            and set_assgnmt.active_date = sps.sample_date

    left join {{ ref('int_diagnostics__constant_occupancy') }} as co
        on
            sps.station_id = co.station_id and sps.lane = co.lane and sps.sample_date = co.sample_date
    left join district_feed_check as dfc
        on set_assgnmt.district = dfc.district
)

select * from detector_status

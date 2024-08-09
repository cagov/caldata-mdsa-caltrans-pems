

with

source as (
    select * from ANALYTICS_PRD.diagnostics.int_diagnostics__samples_per_detector
    where 
    1=1
    
),

district_feed_check as (
    select
        source.district,
        case
            when (count_if(source.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as district_feed_working
    from source
    inner join ANALYTICS_PRD.clearinghouse.districts as d
        on source.district = d.district_id
    group by source.district
),

detector_status as (
    select
        set_assgnmt.active_date,
        set_assgnmt.station_id,
        set_assgnmt.district,
        set_assgnmt.station_type,
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
            when sps.sample_ct between 1 and (0.6 * (2 * 60 * 17))
                then 'Insufficient Data'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.zero_vol_ct / (2 * 60 * 17)
                > (set_assgnmt.zero_flow_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_ct / (2 * 60 * 17)
                > (set_assgnmt.zero_occupancy_percent / 100)
                then 'Card Off'
            when
                set_assgnmt.station_diagnostic_method_id = 'ramp'
                and sps.high_volume_ct / (2 * 60 * 17)
                > (set_assgnmt.high_flow_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.high_occupancy_ct / (2 * 60 * 17)
                > (set_assgnmt.high_occupancy_percent / 100)
                then 'High Val'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_vol_pos_occ_ct / (2 * 60 * 17)
                > (set_assgnmt.flow_occupancy_percent / 100)
                then 'Intermittent'
            when
                set_assgnmt.station_diagnostic_method_id = 'mainline'
                and sps.zero_occ_pos_vol_ct / (2 * 60 * 17)
                > (set_assgnmt.occupancy_flow_percent / 100)
                then 'Intermittent'
            when
                coalesce(co.min_occupancy_delta = 0, false)
                and set_assgnmt.station_diagnostic_method_id = 'mainline'
                then 'Constant'
            --Feed unstable case needed
            else 'Good'
        end as status

    from ANALYTICS_PRD.diagnostics.int_diagnostics__det_diag_set_assignment as set_assgnmt
    left join source as sps
        on
            set_assgnmt.station_id = sps.station_id
            and set_assgnmt.active_date = sps.sample_date

    left join ANALYTICS_PRD.diagnostics.int_diagnostics__constant_occupancy as co
        on
            sps.station_id = co.station_id and sps.lane = co.lane and sps.sample_date = co.sample_date
    left join district_feed_check as dfc
        on set_assgnmt.district = dfc.district
)

select * from detector_status
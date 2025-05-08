{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="active_date",
    snowflake_warehouse=get_snowflake_refresh_warehouse()
) }}

with
source as (
    select
        sample_date as active_date,
        * exclude sample_date
    from {{ ref('int_diagnostics__samples_per_detector') }}
),

detector_meta as (
    select * from {{ ref('int_vds__active_detectors') }}
),

station_meta as (
    select * from {{ ref('int_vds__station_config') }}
),

controller_meta as (
    select * from {{ ref('int_vds__controller_config') }}
),

equipment_meta as (
    select
        dm.*,
        sm.controller_id,
        sm.name,
        sm.angle,
        cm.line_num,
        cm.stn_address,
        cm.controller_type
    from detector_meta as dm
    inner join station_meta as sm
        on
            dm.station_id = sm.station_id
            and {{ get_scd_2_data('dm.active_date','sm._valid_from','sm._valid_to') }}
    inner join controller_meta as cm
        on
            sm.controller_id = cm.controller_id
            and {{ get_scd_2_data('dm.active_date','cm._valid_from','cm._valid_to') }}
    -- Constrain dates to not exceed those in the samples_per_detector model.
    where dm.active_date between (select min(active_date) from source) and (select max(active_date) from source)
),

equipment_with_samples as (
    select
        em.*,
        source.sample_ct
    from equipment_meta as em
    left join source
        on
            em.detector_id = source.detector_id
            and em.active_date = source.active_date
),

district_feed_check as (
    select
        ews.active_date,
        ews.district,
        case
            when (count_if(ews.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as district_feed_working
    from equipment_with_samples as ews
    inner join {{ ref('districts') }} as d
        on ews.district = d.district_id
    group by ews.active_date, ews.district
),

line_feed_check as (
    select
        ews.active_date,
        ews.district,
        ews.line_num,
        case
            when ews.line_num is null then 'Yes'
            when (count_if(ews.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as line_num_working
    from equipment_with_samples as ews
    group by ews.active_date, ews.district, ews.line_num
),

controller_feed_check as (
    select
        ews.active_date,
        ews.district,
        ews.controller_id,
        case
            when ews.controller_id is null then 'Yes'
            when (count_if(ews.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as controller_feed_working
    from equipment_with_samples as ews
    group by ews.active_date, ews.district, ews.controller_id
),

station_feed_check as (
    select
        ews.active_date,
        ews.district,
        ews.station_id,
        case
            when ews.station_id is null then 'Yes'
            when (count_if(ews.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as station_feed_working
    from equipment_with_samples as ews
    group by ews.active_date, ews.district, ews.station_id
),

detector_feed_check as (
    select
        ews.active_date,
        ews.detector_id,
        case
            when ews.detector_id is null then 'Yes'
            when (count_if(ews.sample_ct > 0)) > 0 then 'Yes'
            else 'No'
        end as detector_feed_working
    from equipment_with_samples as ews
    group by ews.active_date, ews.detector_id
),

data_feed_check as (
    select
        ews.active_date,
        ews.district,
        ews.line_num,
        ews.controller_id,
        ews.station_id,
        ews.detector_id,
        ews.sample_ct,
        dfc.district_feed_working,
        lfc.line_num_working,
        cfc.controller_feed_working,
        sfc.station_feed_working,
        detfc.detector_feed_working
    from equipment_with_samples as ews
    left join district_feed_check as dfc
        on
            ews.active_date = dfc.active_date
            and ews.district = dfc.district
    left join line_feed_check as lfc
        on
            ews.active_date = lfc.active_date
            and ews.district = lfc.district
            and ews.line_num = lfc.line_num
    left join controller_feed_check as cfc
        on
            ews.active_date = cfc.active_date
            and ews.district = cfc.district
            and ews.controller_id = cfc.controller_id
    left join station_feed_check as sfc
        on
            ews.active_date = sfc.active_date
            and ews.district = sfc.district
            and ews.station_id = sfc.station_id
    left join detector_feed_check as detfc
        on
            ews.active_date = detfc.active_date
            and ews.detector_id = detfc.detector_id
)

select * from data_feed_check
order by active_date, district, line_num, controller_id, station_id, detector_id

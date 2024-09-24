{{ config(materialized="table") }}

with station_meta as (
    select * from {{ ref('int_vds__station_config') }}
),

mainline as (
    select
        station_id as ml_station_id,
        station_type,
        district,
        city,
        county,
        freeway,
        direction,
        absolute_postmile,
        physical_lanes as ml_lanes,
        _valid_from,
        _valid_to
    from station_meta
    where station_type in ('ML')
),

hov as (
    select
        station_id as hov_station_id,
        station_type,
        district,
        freeway,
        direction,
        name as hov_name,
        latitude as hov_lat,
        longitude as hov_long,
        absolute_postmile,
        length as hov_length,
        physical_lanes as hov_lanes,
        _valid_from,
        _valid_to
    from station_meta
    where station_type in ('HV')
),

station_pairs as (
    select
        a.ml_station_id,
        b.hov_station_id,
        b.hov_length,
        a.ml_lanes,
        b.hov_name,
        b.hov_lanes,
        b.hov_lat,
        b.hov_long,
        a.district,
        a.city,
        a.county,
        a.freeway,
        a.direction,
        abs(b.absolute_postmile - a.absolute_postmile) as delta_postmile,
        a._valid_from,
        a._valid_to
    from mainline as a
    inner join hov as b
        on
            a.freeway = b.freeway
            and a.direction = b.direction
            and a._valid_from = b._valid_from
            and a.ml_station_id != b.hov_station_id
),

closest_stations as (
    select
        *,
        row_number() over (partition by ml_station_id, _valid_from order by delta_postmile asc) as distance_ranking
    from station_pairs
),

closest_station_with_selection as (
    select *
    from closest_stations
    where
        distance_ranking = 1
        and delta_postmile <= 5
),

hourly_station_volume as (
    select
        station_id,
        direction,
        sample_date,
        sample_hour,
        hourly_volume,
        hourly_vmt,
        hourly_vht,
        station_type
    from {{ ref('int_performance__station_metrics_agg_hourly') }}
    where sample_date = dateadd('day', -25, current_date())
),

hourly_station_ml_metrics as (
    select *
    from hourly_station_volume
    where station_type = 'ML'
),

hourly_station_hov_metrics as (
    select *
    from hourly_station_volume
    where station_type = 'HV'
),

station_with_ml_metrics as (
    select
        ax.*,
        bx.sample_date,
        bx.sample_hour,
        bx.hourly_volume as ml_hourly_volume,
        bx.hourly_vmt as ml_hourly_vmt,
        bx.hourly_vht as ml_hourly_vht
    from closest_station_with_selection as ax
    inner join hourly_station_ml_metrics as bx
        on
            ax.ml_station_id = bx.station_id
            and ax.direction = bx.direction
),

station_with_ml_hov_metrics as (
    select
        ay.*,
        cy.hourly_volume as hov_hourly_volume,
        cy.hourly_vmt as hov_hourly_vmt,
        cy.hourly_vht as hov_hourly_vht
    from station_with_ml_metrics as ay
    inner join hourly_station_hov_metrics as cy
        on
            ay.hov_station_id = cy.station_id
            and ay.direction = cy.direction
            and ay.sample_date = cy.sample_date
            and ay.sample_hour = cy.sample_hour
),

station_metric_agg as (
    select
        hov_station_id,
        hov_name,
        hov_lat,
        hov_long,
        avg(hov_length) as hov_length_avg,
        district,
        city,
        county,
        freeway,
        direction,
        sample_date,
        sample_hour,
        (sum(ml_hourly_volume) / nullif(sum(ml_lanes), 0)) as ml_hourly_average_volume,
        (sum(ml_hourly_vmt) / nullif(sum(ml_lanes), 0)) as ml_hourly_average_vmt,
        (sum(ml_hourly_vht) / nullif(sum(ml_lanes), 0)) as ml_hourly_average_vht,
        (sum(hov_hourly_volume) / nullif(sum(hov_lanes), 0)) as hov_hourly_average_volume,
        (sum(hov_hourly_vmt) / nullif(sum(hov_lanes), 0)) as hov_hourly_average_vmt,
        (sum(hov_hourly_vht) / nullif(sum(hov_lanes), 0)) as hov_hourly_average_vht,
        (sum(hov_hourly_volume) / nullif(sum(ml_hourly_volume), 0)) * 100 as hov_volume_penetration,
        (sum(hov_hourly_vht) / nullif(sum(hov_hourly_vmt), 0)) * 100 as hov_vmt_penetration,
        (sum(hov_hourly_vht) / nullif(sum(ml_hourly_vht), 0)) * 100 as hov_vht_penetration
    from station_with_ml_hov_metrics
    group by
        hov_station_id,
        hov_name,
        hov_lat,
        hov_long,
        district,
        city,
        county,
        freeway,
        direction,
        sample_date,
        sample_hour
),

final_data_with_catagory as (
    select
        station_metric_agg.*,
        case
            when extract(hour from station_metric_agg.sample_hour) between 6 and 8 then 'Morning Peak'
            when extract(hour from station_metric_agg.sample_hour) between 9 and 11 then 'Morning Off Peak'
            when extract(hour from station_metric_agg.sample_hour) between 12 and 14 then 'Afternoon Off Peak'
            when extract(hour from station_metric_agg.sample_hour) between 15 and 18 then 'Afternoon Peak'
            else 'Night Off Peak'
        end as hourly_category,

        -- Weekday/Weekend Category
        case
            -- Monday to Friday
            when extract(dayofweek from station_metric_agg.sample_date) in (2, 3, 4, 5, 6) then 'Weekday'
            else 'Weekend'
        end as weekday_status,

        case
            when extract(dayofweek from station_metric_agg.sample_date) = 1 then 'Sunday'
            when extract(dayofweek from station_metric_agg.sample_date) = 2 then 'Monday'
            when extract(dayofweek from station_metric_agg.sample_date) = 3 then 'Tuesday'
            when extract(dayofweek from station_metric_agg.sample_date) = 4 then 'Wednesday'
            when extract(dayofweek from station_metric_agg.sample_date) = 5 then 'Thursday'
            when extract(dayofweek from station_metric_agg.sample_date) = 6 then 'Friday'
            when extract(dayofweek from station_metric_agg.sample_date) = 7 then 'Saturday'
        end as weekday
    from station_metric_agg
)

select * from final_data_with_catagory

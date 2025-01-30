with station_meta as (
    select * from {{ ref('int_vds__active_stations') }}
    where active_date >= dateadd('day', -8, current_date())
),

station_county as (
    {{ get_county_name('station_meta') }}
),

station_pairs as (
    select
        ml.station_id as ml_station_id,
        ml.district,
        ml.city,
        ml.county,
        ml.freeway,
        ml.direction,
        ml.absolute_postmile as ml_absolute_postmile,
        ml.physical_lanes as ml_lanes,
        ml.active_date,
        hov.station_id as hov_station_id,
        hov.name as hov_name,
        hov.latitude as hov_latitude,
        hov.longitude as hov_longitude,
        hov.absolute_postmile as hov_absolute_postmile,
        hov.length as hov_length,
        hov.physical_lanes as hov_lanes,
        abs(ml.absolute_postmile - hov.absolute_postmile) as delta_postmile
    from
        station_county as ml
    inner join
        station_county as hov
        on
            ml.freeway = hov.freeway
            and ml.direction = hov.direction
            and ml.active_date = hov.active_date
            and ml.station_id != hov.station_id
            and ml.station_type = 'ML'
            and hov.station_type = 'HV'
            and ml.district != 4
            and hov.district != 4
),

closest_station_with_selection as (
    select
        *,
        row_number() over (partition by ml_station_id, active_date order by delta_postmile asc) as distance_ranking
    from station_pairs
    where
        delta_postmile <= 5
    qualify
        distance_ranking = 1
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
    where sample_date >= dateadd('day', -8, current_date())
),

/*
 * Join station metadata pairs with hourly observations. This could be
 * expressed more naturally with 2 joins, 1 for ML stations and 1 for HOV,
 * but doing so incurs a significant performance cost. Instead, we have a
 * more permissive join and an aggregation.
 */
station_with_ml_hov_metrics as (
    select
        css.*,
        hourly.sample_date,
        hourly.sample_hour,
        max(case when hourly.station_type = 'ML' then hourly.hourly_volume end) as ml_hourly_volume,
        max(case when hourly.station_type = 'ML' then hourly.hourly_vmt end) as ml_hourly_vmt,
        max(case when hourly.station_type = 'ML' then hourly.hourly_vht end) as ml_hourly_vht,
        max(case when hourly.station_type = 'HV' then hourly.hourly_volume end) as hov_hourly_volume,
        max(case when hourly.station_type = 'HV' then hourly.hourly_vmt end) as hov_hourly_vmt,
        max(case when hourly.station_type = 'HV' then hourly.hourly_vht end) as hov_hourly_vht
    from closest_station_with_selection as css
    inner join hourly_station_volume as hourly
        on
            (css.ml_station_id = hourly.station_id or css.hov_station_id = hourly.station_id)
            and css.direction = hourly.direction
            and css.active_date = hourly.sample_date
    group by all
),

station_metric_agg as (
    select
        hov_station_id,
        hov_name,
        hov_latitude,
        hov_longitude,
        avg(hov_length) as hov_length_avg,
        max(hov_absolute_postmile) as hov_absolute_postmile,
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
        (sum(hov_hourly_vmt) / nullif(sum(ml_hourly_vmt), 0)) * 100 as hov_vmt_penetration,
        (sum(hov_hourly_vht) / nullif(sum(ml_hourly_vht), 0)) * 100 as hov_vht_penetration
    from station_with_ml_hov_metrics
    group by
        hov_station_id,
        hov_name,
        hov_latitude,
        hov_longitude,
        district,
        city,
        county,
        freeway,
        direction,
        sample_date,
        sample_hour
),

final_data_with_category as (
    select
        station_metric_agg.*,

        -- Hourly Category (Based on peak/off-peak periods)
        case
            -- AM Peak
            when
                to_time(station_metric_agg.sample_hour) between to_time({{ var("am_peak_start") }}) and to_time(
                    {{ var("am_peak_end") }}
                )
                then 'am_peak'

            -- Day Off-Peak
            when
                to_time(station_metric_agg.sample_hour) between to_time({{ var("day_off_peak_start") }}) and to_time(
                    {{ var("day_off_peak_end") }}
                )
                then 'day_off_peak'

            -- PM Peak
            when
                to_time(station_metric_agg.sample_hour) between to_time({{ var("pm_peak_start") }}) and to_time(
                    {{ var("pm_peak_end") }}
                )
                then 'pm_peak'

            -- Night Off-Peak (default)
            else 'night_off_peak'
        end as hourly_category,

        -- Weekday/Weekend Category
        case
            when dayofweek(station_metric_agg.sample_date) in (1, 2, 3, 4, 5) then 'Weekday'
            else 'Weekend'
        end as weekday_status,

        -- Day of the Week Name
        case
            when station_metric_agg.sample_date is null then 'No Date'
            when dayofweek(station_metric_agg.sample_date) = 0 then 'Sunday'
            when dayofweek(station_metric_agg.sample_date) = 1 then 'Monday'
            when dayofweek(station_metric_agg.sample_date) = 2 then 'Tuesday'
            when dayofweek(station_metric_agg.sample_date) = 3 then 'Wednesday'
            when dayofweek(station_metric_agg.sample_date) = 4 then 'Thursday'
            when dayofweek(station_metric_agg.sample_date) = 5 then 'Friday'
            when dayofweek(station_metric_agg.sample_date) = 6 then 'Saturday'
            else 'Unknown' -- This should not normally happen
        end as weekday
    from station_metric_agg
)

select
    *,
    extract(hour from sample_hour) as hour_day
from final_data_with_category

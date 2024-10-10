with station_meta as (
    select * from {{ ref('int_vds__station_config') }}
),

county as (
    select
        county_id,
        county_name
    from {{ ref('counties') }}
),

station_with_county as (
    -- Perform the join between station_meta and county on county_id
    select
        sm.* exclude (county),
        c.county_name
    from
        station_meta as sm
    inner join
        county as c
        on sm.county = c.county_id
),

station_pairs as (
    select
        ml.station_id as ml_station_id,
        ml.district,
        ml.city,
        ml.county_name as county,
        ml.freeway,
        ml.direction,
        ml.absolute_postmile as ml_absolute_postmile,
        ml.physical_lanes as ml_lanes,
        ml._valid_from as ml_valid_from,
        ml._valid_to as ml_valid_to,
        hov.station_id as hov_station_id,
        hov.name as hov_name,
        hov.latitude as hov_latitude,
        hov.longitude as hov_longitude,
        hov.absolute_postmile as hov_absolute_postmile,
        hov.length as hov_length,
        hov.physical_lanes as hov_lanes,
        abs(ml.absolute_postmile - hov.absolute_postmile) as delta_postmile
    from
        station_with_county as ml
    inner join
        station_with_county as hov
        on
            ml.freeway = hov.freeway
            and ml.direction = hov.direction
            and ml._valid_from = hov._valid_from
            and ml.station_id != hov.station_id
            and ml.station_type = 'ML'
            and hov.station_type = 'HV'
            and ml.district != 4
            and hov.district != 4
),

closest_station_with_selection as (
    select
        *,
        row_number() over (partition by ml_station_id, ml_valid_from order by delta_postmile asc) as distance_ranking
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

station_with_ml_hov_metrics as (
    select
        ax.*,
        ml.sample_date,
        ml.sample_hour,
        ml.hourly_volume as ml_hourly_volume,
        ml.hourly_vmt as ml_hourly_vmt,
        ml.hourly_vht as ml_hourly_vht,
        hov.hourly_volume as hov_hourly_volume,
        hov.hourly_vmt as hov_hourly_vmt,
        hov.hourly_vht as hov_hourly_vht
    from closest_station_with_selection as ax
    inner join hourly_station_volume as ml
        on
            ax.ml_station_id = ml.station_id
            and ax.direction = ml.direction
            and ml.station_type = 'ML'
    inner join hourly_station_volume as hov
        on
            ax.hov_station_id = hov.station_id
            and ax.direction = hov.direction
            and ml.sample_date = hov.sample_date
            and ml.sample_hour = hov.sample_hour
            and hov.station_type = 'HV'
),

station_metric_agg as (
    select
        hov_station_id,
        hov_name,
        hov_latitude,
        hov_longitude,
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
        (sum(hov_hourly_vmt) / nullif(sum(hov_hourly_vmt), 0)) * 100 as hov_vmt_penetration,
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

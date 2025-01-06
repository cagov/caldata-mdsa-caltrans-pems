{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["detector_id", "sample_timestamp"],
    on_schema_change="append_new_columns",
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}

/*We dynamically select dataset for last month and calculate the statistics (mean, std)
for outlier detection*/
with
five_minute_agg_lastmonth as (
    select
        detector_id,
        sample_date,
        volume_sum,
        occupancy_avg
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where
        sample_date >= dateadd(month, -1, date_trunc('month', current_date))
        and sample_date < date_trunc('month', current_date)
        and station_type in ('ML', 'HV')
),

-- get all good detectors
good_detectors as (
    select
        detector_id,
        sample_date
    from {{ ref("int_diagnostics__detector_status") }}
    where status = 'Good'
),

-- filter last month's data for good detectors only
filtered_five_minute_agg as (
    select
        f.detector_id,
        f.sample_date,
        f.volume_sum,
        f.occupancy_avg
    from five_minute_agg_lastmonth as f
    inner join good_detectors as g
        on
            f.detector_id = g.detector_id
            and f.sample_date = g.sample_date

),

-- calculate the statistics
monthly_stats as (
    select
        detector_id,
        avg(volume_sum) as volume_mean,
        stddev(volume_sum) as volume_stddev,
        -- consider using max_capacity
        percentile_cont(0.95) within group (order by volume_sum) as volume_95th,
        percentile_cont(0.95) within group (order by occupancy_avg) as occupancy_95th
    from filtered_five_minute_agg
    group by detector_id
),

-- retrieve recent five-minute data
five_minute_agg as (
    select
        detector_id,
        sample_date,
        sample_timestamp,
        station_id,
        lane,
        station_type,
        volume_sum,
        occupancy_avg
    from {{ ref('int_clearinghouse__detector_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

-- impute detected outliers
outlier_removed_data as (
    select
        fa.*,
        -- update volume_sum if it's an outlier
        case
            when
                fa.volume_sum - ms.volume_mean / nullifzero(ms.volume_stddev) > 3
                then coalesce(ms.volume_95th, 173)
            else fa.volume_sum
        end as updated_volume_sum,
        -- add a volume_label for imputed volume
        case
            when
                fa.volume_sum - ms.volume_mean / nullifzero(ms.volume_stddev) > 3
                then 'outlier'
            else 'observed data'
        end as volume_label,
        -- update occupancy if it's an outlier
        case
            when
                fa.occupancy_avg > ms.occupancy_95th
                then coalesce(ms.occupancy_95th, 0.8)
            else fa.occupancy_avg
        end as updated_occupancy_avg,
        -- add a column for imputed occupancy
        case
            when
                fa.occupancy_avg > ms.occupancy_95th
                then 'outlier'
            else 'observed data'
        end as occupancy_label
    from five_minute_agg as fa
    left join
        monthly_stats as ms
        on
            fa.detector_id = ms.detector_id

)

select * from outlier_removed_data

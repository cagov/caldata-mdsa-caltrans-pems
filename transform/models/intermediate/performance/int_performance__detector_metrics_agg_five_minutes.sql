{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["detector_id", "sample_timestamp"],
    on_schema_change="sync_all_columns",
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XL")
) }}

with
five_minute_agg as (
    select
        station_id,
        lane,
        detector_id,
        sample_date,
        sample_timestamp,
        district,
        county,
        city,
        freeway,
        direction,
        length,
        sample_ct,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        station_type,
        absolute_postmile,
        volume_imputation_method,
        speed_imputation_method,
        occupancy_imputation_method,
        station_valid_from,
        station_valid_to
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
    where {{ make_model_incremental('sample_date') }}
),

/* Conduct monthly high flow and occupancy detection based on z-score and percentile, and correct outliers
by imputing values based on the 95th percentile data
*/

monthly_stats as (
    select
        detector_id,
        date_trunc('month', sample_date) as month,
        avg(volume_sum) as volume_mean,
        stddev(volume_sum) as volume_stddev,
        -- consider using max_capacity
        percentile_cont(0.95) within group (order by volume_sum) as volume_95th,
        percentile_cont(0.95) within group (order by occupancy_avg) as occupancy_95th
    from five_minute_agg
    group by detector_id, date_trunc('month', sample_date)
),

outlier_removed_data as (
    select
        fa.*,
        -- update volume_sum if it's an outlier
        case
            when
                abs(fa.volume_sum - ms.volume_mean) / nullifzero(ms.volume_stddev) > 3
                and fa.volume_imputation_method in ('observed', 'observed_unimputed')
                then ms.volume_95th
            else fa.volume_sum
        end as volume_sum,
        -- update volume sum imputation method
        case
            when
                abs(fa.volume_sum - ms.volume_mean) / nullifzero(ms.volume_stddev) > 3
                and fa.volume_imputation_method in ('observed', 'observed_unimputed')
                then 'outlier'
            else fa.volume_imputation_method
        end as volume_imputation_method,
        -- update occupancy if it's an outlier
        case
            when
                fa.occupancy_avg > ms.occupancy_95th
                and fa.occupancy_imputation_method in ('observed', 'observed_unimputed')
                then ms.occupancy_95th
            else fa.occupancy_avg
        end as occupancy_avg,
        case
            when
                fa.occupancy_avg > ms.occupancy_95th
                and fa.occupancy_imputation_method in ('observed', 'observed_unimputed')
                then 'outlier'
            else fa.occupancy_imputation_method
        end as occupancy_imputation_method
    from five_minute_agg as fa
    left join monthly_stats as ms
        on
            fa.detector_id = ms.detector_id
            and date_trunc('month', fa.sample_date) = ms.month
),

vmt_vht_metrics as (
    select
        *,
        --vehicle-miles/5-min
        volume_sum * length as vmt,
        --vehicle-hours/5-min
        volume_sum * length / nullifzero(speed_five_mins) as vht,
        --q is in miles per hour for single station
        vmt / nullifzero(vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti
    from outlier_removed_data
),

delay_metrics as (
    select
        vvm.*,
        /*  The formula for delay is: F * (L/V - L/V_t). F = flow (volume),
        L = length of the segment, V = current speed, and V_t = threshold speed. */
        {% for value in var("V_t") %}
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / {{ value }})), 0)
                as delay_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}

    from vmt_vht_metrics as vvm
),

productivity_metrics as (
    select
        dm.*,
        /*
        The formula for Productivity is: Length * (1 - (actual flow / flow capacity))
        */
        {% for value in var("V_t") %}
            case
                when dm.speed_five_mins >= {{ value }}
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_{{ value }}_mph
            {% if not loop.last %}
                ,
            {% endif %}

        {% endfor %}

    from delay_metrics as dm
    inner join {{ ref("int_performance__max_capacity") }} as mc
        on dm.detector_id = mc.detector_id
)

select * from productivity_metrics

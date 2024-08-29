{{ config(
    materialized="incremental",
    cluster_by=["sample_date"],
    unique_key=["detector_id", "sample_timestamp","sample_date"],
    on_schema_change="append_new_columns",
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}
{% set n_lanes = 8 %}

with raw as (
    select
        *,
        /* Create a timestamp truncated down to the nearest five
         minute bucket. This will be the the timestamp on which
         we aggregate. If a 30-second interval straddles two different
         buckets, it will be assigned to the one latter one due to
         the floor() call.
        */
        dateadd(
            'minute',
            floor(minute(sample_timestamp) / 5) * 5,
            trunc(sample_timestamp, 'hour')
        ) as sample_timestamp_trunc
    from {{ ref('stg_clearinghouse__station_raw') }}

    where {{ make_model_incremental('sample_date') }}
),

dmeta as (
    select * from {{ ref('int_vds__detector_config') }}
),

agg as (
    select
        id as station_id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        district,
        {% for lane in range(1, n_lanes+1) %}
            sum(volume_{{ lane }}) as volume_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(volume_{{ lane }} = 0) as zero_vol_ct_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            avg(occupancy_{{ lane }}) as occupancy_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(occupancy_{{ lane }} = 0) as zero_occ_ct_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(volume_{{ lane }} = 0 and occupancy_{{ lane }} > 0) as zero_vol_pos_occ_ct_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(volume_{{ lane }} > 0 and occupancy_{{ lane }} = 0) as zero_occ_pos_vol_ct_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(volume_{{ lane }} > {{ var("high_volume_threshold") }}) as high_volume_ct_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(occupancy_{{ lane }} > {{ var("high_occupancy_threshold") }}) as high_occupancy_ct_{{ lane }},
        {% endfor %}
        {% for lane in range(1, n_lanes+1) %}
            count_if(volume_{{ lane }} is not null and occupancy_{{ lane }} is not null) as sample_ct_{{ lane }},
        {% endfor %}
    {% for lane in range(1, n_lanes+1) %}
        sum(volume_{{ lane }} * speed_{{ lane }})
        / nullifzero(sum(volume_{{ lane }})) as speed_weighted_{{ lane }}        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    from raw
    group by station_id, sample_date, sample_timestamp_trunc, district
),

{% for lane in range(1, n_lanes+1) %}
    agg_{{ lane }} as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_{{ lane }} as sample_ct,
            {{ lane }} as lane,
            volume_{{ lane }} as volume_observed,
            round(iff(
                sample_ct_{{ lane }} >= 10, volume_{{ lane }},
                10 / nullifzero(sample_ct_{{ lane }}) * volume_{{ lane }}
            ))
                as volume_sum, --Represents the normalized flow value
            zero_vol_ct_{{ lane }} as zero_vol_ct,
            occupancy_{{ lane }} as occupancy_avg,
            zero_occ_ct_{{ lane }} as zero_occ_ct,
            zero_vol_pos_occ_ct_{{ lane }} as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_{{ lane }} as zero_occ_pos_vol_ct,
            high_volume_ct_{{ lane }} as high_volume_ct,
            high_occupancy_ct_{{ lane }} as high_occupancy_ct,
            speed_weighted_{{ lane }} as speed_weighted
        from agg
    ),
{% endfor %}

agg_unioned as (
    {% for lane in range(1, n_lanes+1) %}
        select * from agg_{{ lane }}
        {{ "union all" if not loop.last }}
    {% endfor %}
),

agg_with_metadata as (
    select
        agg.*,
        dmeta.detector_id,
        dmeta.state_postmile,
        dmeta.absolute_postmile,
        dmeta.latitude,
        dmeta.longitude,
        dmeta.physical_lanes,
        dmeta.station_type,
        dmeta.county,
        dmeta.city,
        dmeta.freeway,
        dmeta.direction,
        dmeta.length,
        dmeta._valid_from as station_valid_from,
        dmeta._valid_to as station_valid_to
    from agg_unioned as agg inner join dmeta
        on
            agg.station_id = dmeta.station_id
            and agg.lane = dmeta.lane
            and {{ get_scd_2_data('agg.sample_date','dmeta._valid_from','dmeta._valid_to') }}

)

select * from agg_with_metadata

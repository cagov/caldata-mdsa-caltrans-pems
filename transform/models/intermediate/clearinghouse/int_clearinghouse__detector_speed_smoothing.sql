{{ config(
    materialized="incremental",
    unique_key=["detector_id", "sample_timestamp"],
    snowflake_warehouse = get_snowflake_refresh_warehouse(small="XS", big="XL")
) }}


with

recursive smoothed_data as (
    --base case
    select
        detector_id,
        sample_timestamp,
        speed_preliminary,
        p_factor,
        speed_preliminary as speed_smoothed,
        row_number() over (partition by detector_id order by sample_timestamp) as rn
    from {{ ref('int_clearinghouse__detector_g_factor_based_speed') }}
    qualify rn = 1

    union all

    -- recursive cases
    select
        g.detector_id,
        g.sample_timestamp,
        g.speed_preliminary,
        g.p_factor,
        g.p_factor * g.speed_preliminary
        + (1 - g.p_factor) * coalesce(sd.speed_smoothed, g.speed_preliminary) as speed_smoothed,
        g.rn
    from (
        select
            detector_id,
            sample_timestamp,
            speed_preliminary,
            p_factor,
            row_number() over (partition by detector_id order by sample_timestamp) as rn
        from {{ ref('int_clearinghouse__detector_g_factor_based_speed') }}
    ) as g
    inner join
        smoothed_data as sd
        on g.detector_id = sd.detector_id and g.rn = sd.rn + 1
)

select * from smoothed_data
order by detector_id, sample_timestamp

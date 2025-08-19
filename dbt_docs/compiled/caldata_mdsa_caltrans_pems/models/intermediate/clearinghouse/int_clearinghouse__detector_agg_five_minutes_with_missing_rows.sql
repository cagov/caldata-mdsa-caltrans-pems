

with timestamp_spine as (
    

  
  

  with date_spine as (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 961
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        '2023-01-01'
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= current_date()

)

select * from filtered


  ),

  series as (
    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
    
    

    )

    select *
    from unioned
    where generated_number <= 288.0
    order by generated_number


  )

  select DATEADD(s, (generated_number - 1) * 300, date_day) as timestamp_column
  from date_spine
  cross join series


),

detector_agg as (
    select * from ANALYTICS_PRD.clearinghouse.int_clearinghouse__detector_agg_five_minutes
    -- This clause isn't strictly necessary but helps with performance on incremental builds
    where 
    1=1
    
),

detector_meta as (
    select * from ANALYTICS_PRD.vds.int_vds__detector_config
),

/* Get date range where a detector is expected to be collecting data. */
detector_date_range as (
    select
        detector_id,
        active_date as sample_date
    from ANALYTICS_PRD.vds.int_vds__active_detectors
    where 
    1=1
    
),

/* Expand timestamp spine to include values per detector but only for days within the detector's date range */
spine as (
    select
        ts.timestamp_column,
        dd.detector_id
    from timestamp_spine as ts inner join detector_date_range as dd
        on to_date(ts.timestamp_column) = dd.sample_date
),

-- Add the model where gfactor speed has been calculated
gfactor_speed as (
    select
        detector_id,
        sample_timestamp,
        imputed_speed
    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__detector_g_factor_based_speed
),

/* Join 5-minute aggregated data to the spine to get a table without missing rows */
base as (
    select
        spine.detector_id,
        coalesce(agg.sample_date, to_date(spine.timestamp_column)) as sample_date,
        coalesce(agg.sample_timestamp, spine.timestamp_column) as sample_timestamp,
        coalesce(agg.station_id, dmeta.station_id) as station_id,
        coalesce(agg.district, dmeta.district) as district,
        agg.sample_ct,
        coalesce(agg.lane, dmeta.lane) as lane,
        agg.volume_sum,
        agg.zero_vol_ct,
        agg.occupancy_avg,
        agg.zero_occ_ct,
        agg.zero_vol_pos_occ_ct,
        agg.zero_occ_pos_vol_ct,
        agg.high_volume_ct,
        agg.high_occupancy_ct,
        coalesce(agg.speed_weighted, gs.imputed_speed) as speed_weighted,
        agg.volume_observed,
        coalesce(agg.state_postmile, dmeta.state_postmile) as state_postmile,
        coalesce(agg.absolute_postmile, dmeta.absolute_postmile) as absolute_postmile,
        coalesce(agg.latitude, dmeta.latitude) as latitude,
        coalesce(agg.longitude, dmeta.longitude) as longitude,
        coalesce(agg.physical_lanes, dmeta.physical_lanes) as physical_lanes,
        coalesce(agg.station_type, dmeta.station_type) as station_type,
        coalesce(agg.county, dmeta.county) as county,
        coalesce(agg.city, dmeta.city) as city,
        coalesce(agg.freeway, dmeta.freeway) as freeway,
        coalesce(agg.direction, dmeta.direction) as direction,
        coalesce(agg.length, dmeta.length) as length,
        coalesce(agg.station_valid_from, dmeta._valid_from) as station_valid_from,
        coalesce(agg.station_valid_to, dmeta._valid_to) as station_valid_to
    from spine
    left join detector_agg as agg
        on spine.timestamp_column = agg.sample_timestamp and spine.detector_id = agg.detector_id

    -- The previously "missing" rows will need metadata filled in
    left join detector_meta as dmeta
        on
            agg.sample_ct is null -- this filters for missing rows since it is a computed value in upstream models
            and spine.detector_id = dmeta.detector_id
            and to_date(spine.timestamp_column) >= dmeta._valid_from
            and (
                to_date(spine.timestamp_column) < dmeta._valid_to
                or dmeta._valid_to is null
            )
    left join gfactor_speed as gs
        on
            agg.detector_id = gs.detector_id
            and agg.sample_timestamp = gs.sample_timestamp
)

select * from base
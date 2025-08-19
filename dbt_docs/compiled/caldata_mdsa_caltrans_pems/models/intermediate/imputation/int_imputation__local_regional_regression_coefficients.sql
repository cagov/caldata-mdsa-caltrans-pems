

-- Generate dates using dbt_utils.date_spine
with date_spine as (
    select cast(date_day as date) as regression_date
    from (
        





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
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
     + 
    
    p13.generated_number * power(2, 13)
    
    
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
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
     cross join 
    
    p as p13
    
    

    )

    select *
    from unioned
    where generated_number <= 9819
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        '1998-10-01'
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


    ) as spine
),

-- -- Filter dates to get the desired date sequence
regression_dates as (
    select *
    from date_spine
    where
        extract(day from regression_date) = 3
        and extract(month from regression_date) in (2, 5, 8, 11)
),

regression_dates_to_evaluate as (
    select * from regression_dates
    
),

agg as (
    select *
    from ANALYTICS_PRD.clearinghouse.int_clearinghouse__detector_agg_five_minutes
),

-- Select all station pairs that are active for the chosen regression dates
nearby_stations as (
    select
        nearby.station_id,
        nearby.other_station_id,
        nearby.other_station_is_local,
        regression_dates_to_evaluate.regression_date
    from ANALYTICS_PRD.vds.int_vds__nearby_stations as nearby
    inner join regression_dates_to_evaluate
        on
            

    regression_dates_to_evaluate.regression_date >= nearby._valid_from
    and ( regression_dates_to_evaluate.regression_date < nearby._valid_to or nearby._valid_to is null)



    /* This filters the nearby_stations model further to make sure we don't do the pairwise
    join below on more dates than we need. In theory, the parwise join *should* be able to
    do this filtering already, but in some profiling Snowflake was doing some join reordering
    that caused an unnecessary row explosion, where the date filtering was happening
    after the pairwise join. So this helps avoid that behavior. */
    where regression_dates_to_evaluate.regression_date >= (select min(agg.sample_date) from agg)
),


-- Get all of the detectors that are producing good data, based on
-- the diagnostic tests
good_detectors as (
    select
        detector_id,
        district,
        sample_date
    from ANALYTICS_PRD.diagnostics.int_diagnostics__detector_status
    where status = 'Good'
),

/* Get the five-minute unimputed data. This is joined on the
regression dates to only get samples which are within a week of
the regression date. It's also joined with the "good detectors"
table to only get samples from dates that we think were producing
good data. */
detector_counts as (
    select
        agg.station_id,
        agg.detector_id,
        agg.sample_date,
        agg.sample_timestamp,
        agg.volume_sum,
        agg.occupancy_avg,
        agg.speed_weighted,
        good_detectors.district,
        -- TODO: Can we give this a better name? Can we move this into the base model?
        coalesce(agg.speed_weighted, (agg.volume_sum * 22) / nullifzero(agg.occupancy_avg) * (1 / 5280) * 12)
            as speed_five_mins,
        regression_dates_to_evaluate.regression_date
    from agg
    inner join regression_dates_to_evaluate
        on
            agg.sample_date >= regression_dates_to_evaluate.regression_date
            and agg.sample_date
            < dateadd(day, 7, regression_dates_to_evaluate.regression_date)
    inner join good_detectors
        on
            agg.detector_id = good_detectors.detector_id
            and agg.sample_date = good_detectors.sample_date
),


-- Self-join the 5-minute aggregated data with itself,
-- joining on the whether a station is itself or one
-- of it's neighbors. This is a big table, as we get
-- the product of all of the lanes in nearby stations
detector_counts_pairwise as (
    select
        a.station_id,
        b.station_id as other_station_id,
        a.detector_id,
        b.detector_id as other_detector_id,
        a.district,
        a.regression_date,
        a.speed_five_mins as speed,
        b.speed_five_mins as other_speed,
        a.volume_sum as volume,
        b.volume_sum as other_volume,
        a.occupancy_avg as occupancy,
        b.occupancy_avg as other_occupancy,
        nearby_stations.other_station_is_local
    from detector_counts as a
    left join nearby_stations
        on
            a.station_id = nearby_stations.station_id
            and a.regression_date = nearby_stations.regression_date
    inner join detector_counts as b
        on
            nearby_stations.other_station_id = b.station_id
            and a.sample_date = b.sample_date
            and a.sample_timestamp = b.sample_timestamp
),

-- Aggregate the self-joined table to get the slope
-- and intercept of the regression.
detector_counts_regression as (
    select
        detector_id,
        other_detector_id,
        district,
        regression_date,
        other_station_is_local,
        -- speed regression model
        regr_slope(speed, other_speed) as speed_slope,
        regr_intercept(speed, other_speed) as speed_intercept,
        -- flow or volume regression model
        regr_slope(volume, other_volume) as volume_slope,
        regr_intercept(volume, other_volume) as volume_intercept,
        -- occupancy regression model
        regr_slope(occupancy, other_occupancy) as occupancy_slope,
        regr_intercept(occupancy, other_occupancy) as occupancy_intercept
    from detector_counts_pairwise
    where not (detector_id = other_detector_id)-- don't bother regressing on self!
    group by detector_id, other_detector_id, district, regression_date, other_station_is_local
    -- No point in regressing if the variables are all null,
    -- this can save significant time.
    having
        (count(volume) > 0 and count(other_volume) > 0)
        or (count(occupancy) > 0 and count(other_occupancy) > 0)
        or (count(speed) > 0 and count(other_speed) > 0)
)

select * from detector_counts_regression
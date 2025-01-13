

with

detector_status as (
    select * from ANALYTICS_PRD.diagnostics.int_diagnostics__detector_status
    where sample_date is not null and lane is not null
),

detector_statusc as (
    
    with county as (
        select
            county_id,
            lower(county_name) as county
        from ANALYTICS_PRD.clearinghouse.counties
    ),
    station_with_county as (
        select
            detector_status.* exclude (county),
            c.county
        from detector_status
        inner join county as c
        on detector_status.county = c.county_id
    )

    select * from station_with_county

)

select * from detector_statusc
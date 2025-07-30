

-- Get all of the detectors that are producing good data, based on
-- the diagnostic tests
with good_detectors as (
    select
        station_id,
        lane,
        district,
        sample_date
    from ANALYTICS_PRD.diagnostics.int_diagnostics__detector_status
    where status = 'Good'
),

-- read detector daily aggregated performance metrics
-- join with good dectors
good_detectors_daily_agg as (
    select sd.*
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_daily as sd
    inner join good_detectors on
        sd.station_id = good_detectors.station_id
        and sd.lane = good_detectors.lane
        and sd.sample_date = good_detectors.sample_date
),

--  calculate station level metrics first
good_station_daily_agg as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        -- aggregate station volume over all lanes
        sum(daily_volume) as daily_volume,
        sample_date
    from good_detectors_daily_agg
    group by station_id, city, county, district, freeway, direction, station_type, sample_date
),

-- AADT_1: Arithmetic Mean
aadt_1 as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        avg(daily_volume) as aadt_1,
        date_trunc('year', sample_date) as sample_year
    from good_station_daily_agg
    group by station_id, city, county, district, freeway, direction, station_type, sample_year
),

-- AADT_2: ASTM Standard 1442
madw as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        -- calculate  Monthly Average Days of the Week (MADW).
        avg(daily_volume) as madw,
        extract(dow from sample_date) as day_of_week,
        date_trunc('month', sample_date) as sample_month,
        date_trunc('year', sample_date) as sample_year
    from good_station_daily_agg
    group by
        station_id, city, county, freeway, direction, district, station_type, day_of_week, sample_month, sample_year
),

madt as (
    select
        station_id,
        city,
        county,
        district,
        direction,
        freeway,
        station_type,
        sample_month,
        sample_year,
        count(station_id) as sample_ct,
        avg(madw) as madt
    from madw
    where madw > 0
    group by station_id, city, county, district, freeway, direction, station_type, sample_month, sample_year
),

aadt_2 as (
    select
        station_id,
        city,
        county,
        district,
        direction,
        freeway,
        station_type,
        sample_year,
        count(sample_month) as sample_ct,
        avg(madt) as aadt_2
    from madt
    group by station_id, city, county, district, freeway, direction, station_type, sample_year
    -- all 12 months average traffic volume is required to calculate this metric
    having count(sample_month) = 12
),

-- AADT_3: Conventional AASHTO Procedures
aadw as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        -- calculate Annual Average Days of the Week (AADW)
        avg(madw) as aadw,
        day_of_week,
        sample_year
    from madw
    where madw > 0
    group by station_id, city, county, district, freeway, direction, station_type, day_of_week, sample_year
),

aadt_3 as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        sample_year,
        avg(aadw) as aadt_3
    from aadw
    group by station_id, city, county, district, freeway, direction, station_type, sample_year
),

-- read the detector hourly aggregated model
-- filter out the good dectors data only
good_detectors_hourly_agg as (
    select sh.*
    from ANALYTICS_PRD.performance.int_performance__detector_metrics_agg_hourly as sh
    inner join good_detectors on
        sh.station_id = good_detectors.station_id
        and sh.lane = good_detectors.lane
        and sh.sample_date = good_detectors.sample_date
),

--  aggregate the hourly volume in station level first
good_station_hourly_agg as (
    select
        station_id,
        district,
        station_type,
        sample_hour,
        sample_date,
        sum(hourly_volume) as hourly_volume
    from good_detectors_hourly_agg
    group by station_id, sample_date, sample_hour, district, station_type

),

-- AADT_4: Provisional AASHTO Procedures
mahw as (
    select
        station_id,
        district,
        station_type,
        extract(dow from sample_date) as day_of_week,
        extract(hour from sample_hour) as hour_of_day,
        date_trunc('month', sample_date) as sample_month,
        date_trunc('year', sample_date) as sample_year,
        -- calculate monthly average flow for each hour of the week
        avg(hourly_volume) as mahw
    from good_station_hourly_agg
    group by station_id, district, station_type, hour_of_day, day_of_week, sample_month, sample_year
),

monthly_average_days_of_the_week_traffic as (
    select
        station_id,
        district,
        station_type,
        day_of_week,
        sample_month,
        sample_year,
        sum(mahw) as madw
    from mahw
    group by station_id, district, station_type, day_of_week, sample_month, sample_year
),

averages_of_madw as (
    select
        station_id,
        district,
        station_type,
        avg(madw) as aadw,
        day_of_week,
        sample_year
    from monthly_average_days_of_the_week_traffic
    where madw > 0
    group by station_id, district, station_type, day_of_week, sample_year
),

aadt_4 as (
    select
        station_id,
        district,
        station_type,
        avg(aadw) as aadt_4,
        sample_year
    from averages_of_madw
    group by station_id, district, station_type, sample_year
),

-- AADT_5: Sum of 24 Annual Average Hourly Traffic Volumes

annual_average_hourly_traffic as (
    select
        station_id,
        district,
        station_type,
        extract(hour from sample_hour) as hour_of_day,
        date_trunc('year', sample_date) as sample_year,
        avg(hourly_volume) as aaht
    from good_station_hourly_agg
    group by station_id, district, station_type, hour_of_day, sample_year
),

aadt_5 as (
    select
        station_id,
        district,
        station_type,
        sample_year,
        sum(aaht) as aadt_5
    from annual_average_hourly_traffic
    group by station_id, district, station_type, sample_year
),

-- AADT_6: Modified ASTM Standard
aadt_6 as (
    select
        station_id,
        city,
        county,
        district,
        direction,
        freeway,
        station_type,
        sample_year,
        avg(madt) as aadt_6
    from madt
    group by station_id, city, county, district, freeway, direction, station_type, sample_year
    -- 1 of the 12 madt values may be missing for this final AADT calculation.
    having count(madt) >= 11
),

-- AADT_7: Modified Conventional AASHTO
aadw1 as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        -- calculate Annual Average Days of the Week (AADW)
        avg(madw) as aadw,
        count(madw) as sample_ct,
        day_of_week,
        sample_year
    from madw
    where madw > 0
    group by station_id, city, county, district, freeway, direction, station_type, day_of_week, sample_year
    -- 1 of the 12 MADW values may be missing in the AADW subcomputation.
    having count(madw) >= 11
),

aadt_7 as (
    select
        station_id,
        city,
        county,
        district,
        freeway,
        direction,
        station_type,
        sample_year,
        avg(aadw) as aadt_7,
        count(aadw) as sample_ct
    from aadw1
    group by station_id, city, county, district, freeway, direction, station_type, sample_year
    -- 1 of the 7 aadw values may be missing for this final AADT calculation.
    having count(aadw) >= 6
),

-- AADT_8: Modified Provisional AASHTO
averages_of_madw1 as (
    select
        station_id,
        district,
        station_type,
        avg(madw) as aadw,
        day_of_week,
        count(madw) as sample_ct,
        sample_year
    from monthly_average_days_of_the_week_traffic
    where madw > 0
    group by station_id, district, station_type, day_of_week, sample_year
    -- 1 of the 12 MADW values may be missing in the AADW subcomputation
    having count(madw) >= 11
),

aadt_8 as (
    select
        station_id,
        district,
        station_type,
        avg(aadw) as aadt_8,
        sample_year
    from averages_of_madw1
    group by station_id, district, station_type, sample_year
    -- 1 of the 7 aadw values may be missing for this final AADT calculation.
    having count(aadw) >= 6
),

-- now join all aadt_CTE together
aadt_1_8 as (
    select
        aadt_1.*,
        aadt_2.aadt_2,
        aadt_3.aadt_3,
        aadt_4.aadt_4,
        aadt_5.aadt_5,
        aadt_6.aadt_6,
        aadt_7.aadt_7,
        aadt_8.aadt_8
    from aadt_1
    left join aadt_2
        on
            aadt_1.station_id = aadt_2.station_id
            and aadt_1.sample_year = aadt_2.sample_year
    left join aadt_3
        on
            aadt_1.station_id = aadt_3.station_id
            and aadt_1.sample_year = aadt_3.sample_year
    left join aadt_4
        on
            aadt_1.station_id = aadt_4.station_id
            and aadt_1.sample_year = aadt_4.sample_year
    left join aadt_5
        on
            aadt_1.station_id = aadt_5.station_id
            and aadt_1.sample_year = aadt_5.sample_year
    left join aadt_6
        on
            aadt_1.station_id = aadt_6.station_id
            and aadt_1.sample_year = aadt_6.sample_year
    left join aadt_7
        on
            aadt_1.station_id = aadt_7.station_id
            and aadt_1.sample_year = aadt_7.sample_year
    left join aadt_8
        on
            aadt_1.station_id = aadt_8.station_id
            and aadt_1.sample_year = aadt_8.sample_year
),

-- Calculate k-factors
traffic_data as (
    select
        station_id,
        district,
        station_type,
        hourly_volume,
        sample_date,
        date_trunc('year', sample_date) as observation_year
    from good_station_hourly_agg
),

-- get 30th highest hour volume in preceding year
k_30 as (
    select
        station_id,
        district,
        station_type,
        hourly_volume as k_30,
        observation_year,
        dateadd(year, 1, observation_year) as kfactor_year
    from traffic_data
    qualify rank() over (
        partition by station_id, observation_year
        order by hourly_volume desc
    ) = 30
),

-- get 50th highest hour volume in preceding year
k_50 as (
    select
        station_id,
        district,
        station_type,
        hourly_volume as k_50,
        observation_year,
        dateadd(year, 1, observation_year) as kfactor_year
    from traffic_data
    qualify rank() over (
        partition by station_id, observation_year
        order by hourly_volume desc
    ) = 50
),

-- get 100th highest hour volume in preceding year
k_100 as (
    select
        station_id,
        district,
        station_type,
        hourly_volume as k_100,
        observation_year,
        dateadd(year, 1, observation_year) as kfactor_year
    from traffic_data
    qualify rank() over (
        partition by station_id, observation_year
        order by hourly_volume desc
    ) = 100
),

-- join all k factors with all AADT
aadt_1_8_kfactors as (
    select
        aadt_1_8.*,
        k_30.k_30,
        k_50.k_50,
        k_100.k_100
    from aadt_1_8
    left join k_30
        on
            aadt_1_8.station_id = k_30.station_id
            and aadt_1_8.sample_year = k_30.kfactor_year
    left join k_50
        on
            aadt_1_8.station_id = k_50.station_id
            and aadt_1_8.sample_year = k_50.kfactor_year
    left join k_100
        on
            aadt_1_8.station_id = k_100.station_id
            and aadt_1_8.sample_year = k_100.kfactor_year
)

select distinct
    station_id,
    station_type,
    sample_year,
    city,
    county,
    direction,
    district,
    freeway,
    aadt_1,
    aadt_2,
    aadt_3,
    aadt_4,
    aadt_5,
    aadt_6,
    aadt_7,
    aadt_8,
    k_30,
    k_50,
    k_100
from aadt_1_8_kfactors
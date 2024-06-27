{{ config(materialized='table') }}

-- AADT_1: Arithmetic Mean
with aadt_1 as (
    select
        id,
        city,
        county,
        district,
        freeway,
        direction,
        type,
        avg(daily_volume) as aadt_1,
        date_trunc('year', sample_date) as sample_year
    from {{ ref('int_performance__station_metrics_agg_daily') }}
    group by id, city, county, district, freeway, direction, type, sample_year
),

-- AADT_2: ASTM Standard 1442
madw as (
    select
        id,
        city,
        county,
        district,
        freeway,
        direction,
        type,
        -- calculate  Monthly Average Days of the Week (MADW)
        avg(daily_volume) as madw,
        extract(dow from sample_date) as day_of_week,
        date_trunc('month', sample_date) as sample_month,
        date_trunc('year', sample_date) as sample_year
    from {{ ref('int_performance__station_metrics_agg_daily') }}
    group by id, city, county, freeway, direction, district, type, day_of_week, sample_month, sample_year
),

madt as (
    select
        id,
        city,
        county,
        district,
        direction,
        freeway,
        type,
        sample_month,
        sample_year,
        count(id) as sample_ct,
        avg(madw) as madt
    from madw
    group by id, city, county, district, freeway, direction, type, sample_month, sample_year
),

aadt_2 as (
    select
        id,
        city,
        county,
        district,
        direction,
        freeway,
        type,
        sample_year,
        count(sample_month) as sample_ct,
        avg(madt) as aadt_2
    from madt
    group by id, city, county, district, freeway, direction, type, sample_year
    having count(id) >= 12
),

-- AADT_3: Conventional AASHTO Procedures
aadw as (
    select
        id,
        city,
        county,
        district,
        freeway,
        direction,
        type,
        -- calculate Annual Average Days of the Week (AADW)
        avg(madw) as aadw,
        day_of_week,
        sample_year
    from madw
    group by id, city, county, district, freeway, direction, type, day_of_week, sample_year
),

aadt_3 as (
    select
        id,
        city,
        county,
        district,
        freeway,
        direction,
        type,
        sample_year,
        avg(aadw) as aadt_3
    from aadw
    group by id, city, county, district, freeway, direction, type, sample_year
    having count(id) = 7
),

-- AADT_4: Provisional AASHTO Procedures
mahw as (
    select
        id,
        district,
        type,
        extract(dow from sample_date) as day_of_week,
        extract(hour from sample_hour) as hour_of_day,
        date_trunc('month', sample_date) as sample_month,
        date_trunc('year', sample_date) as sample_year,
        -- calculate monthly average flow for each hour of the week
        avg(hourly_volume) as mahw
    from {{ ref('int_performance__station_metrics_agg_hourly') }}
    group by id, district, type, hour_of_day, day_of_week, sample_month, sample_year
),

monthly_average_days_of_the_week_traffic as (
    select
        id,
        district,
        type,
        day_of_week,
        sample_month,
        sample_year,
        sum(mahw) as madw
    from mahw
    group by id, district, type, day_of_week, sample_month, sample_year
),

averages_of_madw as (
    select
        id,
        district,
        type,
        avg(madw) as aadw,
        day_of_week,
        sample_year
    from monthly_average_days_of_the_week_traffic
    group by id, district, type, day_of_week, sample_year
),

aadt_4 as (
    select
        id,
        district,
        type,
        avg(aadw) as aadt_4,
        sample_year
    from averages_of_madw
    group by id, district, type, sample_year
    having count(id) = 7
),

-- AADT_5: Sum of 24 Annual Average Hourly Traffic Volumes

annual_average_hourly_traffic as (
    select
        id,
        district,
        type,
        extract(hour from sample_hour) as hour_of_day,
        date_trunc('year', sample_date) as sample_year,
        avg(hourly_volume) as aaht
    from {{ ref('int_performance__station_metrics_agg_hourly') }}
    group by id, district, type, hour_of_day, sample_year
),

aadt_5 as (
    select
        id,
        district,
        type,
        sample_year,
        sum(aaht) as aadt_5
    from annual_average_hourly_traffic
    group by id, district, type, sample_year
),

-- AADT_6: Modified ASTM Standard
aadt_6 as (
    select
        id,
        city,
        county,
        district,
        direction,
        freeway,
        type,
        sample_year,
        avg(madt) as aadt_6
    from madt
    group by id, city, county, district, freeway, direction, type, sample_year
    having count(id) >= 11
),

-- AADT_7: Modified Conventional AASHTO
aadt_7 as (
    select
        id,
        city,
        county,
        district,
        freeway,
        direction,
        type,
        sample_year,
        avg(aadw) as aadt_7
    from aadw
    group by id, city, county, district, freeway, direction, type, sample_year
    having count(id) >= 6
),

-- AADT_8: Modified Provisional AASHTO
aadt_8 as (
    select
        id,
        district,
        type,
        avg(aadw) as aadt_8,
        sample_year
    from averages_of_madw
    group by id, district, type, sample_year
    having count(id) >= 6
),

-- now join all CTE together
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
            aadt_1.id = aadt_2.id
            and aadt_1.sample_year = aadt_2.sample_year
    left join aadt_3
        on
            aadt_1.id = aadt_3.id
            and aadt_1.sample_year = aadt_3.sample_year
    left join aadt_4
        on
            aadt_1.id = aadt_4.id
            and aadt_1.sample_year = aadt_4.sample_year
    left join aadt_5
        on
            aadt_1.id = aadt_5.id
            and aadt_1.sample_year = aadt_5.sample_year
    left join aadt_6
        on
            aadt_1.id = aadt_6.id
            and aadt_1.sample_year = aadt_6.sample_year
    left join aadt_7
        on
            aadt_1.id = aadt_7.id
            and aadt_1.sample_year = aadt_7.sample_year
    left join aadt_8
        on
            aadt_1.id = aadt_8.id
            and aadt_1.sample_year = aadt_8.sample_year
),

-- Calculate k-factors
traffic_data as (
    select
        id,
        district,
        type,
        hourly_volume,
        sample_date,
        date_trunc('year', sample_date) as observation_year
    from {{ ref('int_performance__station_metrics_agg_hourly') }}
),

k_30 as (
    select
        id,
        district,
        type,
        hourly_volume as k_30,
        observation_year,
        dateadd(year, 1, observation_year) as kfactor_year
    from traffic_data
    qualify rank() over (
        partition by id, observation_year
        order by hourly_volume desc
    ) = 30
),

k_50 as (
    select
        id,
        district,
        type,
        hourly_volume as k_50,
        observation_year,
        dateadd(year, 1, observation_year) as kfactor_year
    from traffic_data
    qualify rank() over (
        partition by id, observation_year
        order by hourly_volume desc
    ) = 50
),

k_100 as (
    select
        id,
        district,
        type,
        hourly_volume as k_100,
        observation_year,
        dateadd(year, 1, observation_year) as kfactor_year
    from traffic_data
    qualify rank() over (
        partition by id, observation_year
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
            aadt_1_8.id = k_30.id
            and aadt_1_8.sample_year = k_30.kfactor_year
    left join k_50
        on
            aadt_1_8.id = k_50.id
            and aadt_1_8.sample_year = k_50.kfactor_year
    left join k_100
        on
            aadt_1_8.id = k_100.id
            and aadt_1_8.sample_year = k_100.kfactor_year
)

select * from aadt_1_8_kfactors

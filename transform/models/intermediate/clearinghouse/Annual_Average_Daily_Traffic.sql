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
    from {{ ref('int_clearinghouse__station_temporal_daily_agg') }}
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
    from {{ ref('int_clearinghouse__station_temporal_daily_agg') }}
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
        count(id) as madt_sample_ct,
        avg(madt) as aadt_2
    from madt
    group by id, city, county, district, freeway, direction, type, sample_year
    having count(id) = 12
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
    from {{ ref('int_clearninghouse__station_temporal_hourly_agg') }}
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
        avg(aadw) as asaadt_4,
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
    from {{ ref('int_clearninghouse__station_temporal_hourly_agg') }}
    group by id, district, type, hour_of_day, sample_year
),

aadt_5 as (
    select
        id,
        district,
        type,
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
        avg(madt) as aadt_2
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
        avg(aadw) as asaadt_8,
        sample_year
    from averages_of_madw
    group by id, district, type, sample_year
    having count(id) >= 6
)

-- now join all CTE together
select * from aadt_8

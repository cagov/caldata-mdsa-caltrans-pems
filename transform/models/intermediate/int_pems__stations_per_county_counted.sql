with 

source as (
    SELECT * FROM {{ ref("stg_pems__station_meta")}}
),

station_county_count as (
    select
        COUNTY,
        COUNT(ID) AS STATION_COUNTS
    from source
    where Year(META_DATE) = 2023
    group by COUNTY
    

)

select * from station_county_count
order by STATION_COUNTS desc
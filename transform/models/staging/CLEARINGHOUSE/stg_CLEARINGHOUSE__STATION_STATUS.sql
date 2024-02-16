with

source as (

    select * from {{ source('CLEARINGHOUSE', 'STATION_STATUS') }}

),

renamed as (

    select
        filename,
        content

    from source

)

select * from renamed

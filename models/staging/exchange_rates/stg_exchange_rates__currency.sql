with 

source as (

    select * from {{ source('exchange_rates', 'currency') }}

),

renamed as (

    select
        code as currency_code,
        name as currency_name

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('exchange_rates', 'rate') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['currency_code','data_base','data_timestamps']) }} as id,
        currency_code as exchange_currency_code,
        data_base as base_currency_code,
        data_timestamps as loaded_at_ts,
        rate as exchange_rate

    from source

)

select * from renamed

{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

with currencies as (

    select *
    from {{ ref('stg_exchange_rates__currency') }}

), exchange_rates as (

    select *
    from {{ ref('stg_exchange_rates__rate') }}
    {% if is_incremental() %}
    where loaded_at_ts >= (select coalesce(max(loaded_at_ts),'1900-01-01') from {{ this }} )
    {% endif %}

), joined as (

    select
        er.id
        ,er.base_currency_code as from_currency_code
        ,"US Dollar" as from_currency
        ,er.exchange_currency_code as to_currency_code
        ,c.currency_name as to_currency
        ,er.exchange_rate as exchange_rate
        ,er.loaded_at_ts
    from exchange_rates as er
    inner join currencies as c
        where er.exchange_currency_code = c.currency_code

), format as (

    select 
        joined.*
        ,{{ dbt.date_trunc("day", "loaded_at_ts") }} as loaded_at_date
        ,{{ dbt.date_trunc("month", "loaded_at_ts") }} as loaded_at_month
        ,{{ dbt.date_trunc("year", "loaded_at_ts") }} as loaded_at_year
    from joined

)

select *
from format

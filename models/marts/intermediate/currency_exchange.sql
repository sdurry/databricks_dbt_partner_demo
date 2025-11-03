with currencies as (

    select *
    from {{ ref('stg_exchange_rates__currency') }}

), exchange_rates as (

    select *
    from {{ ref('stg_exchange_rates__rate') }}

), joined as (

    select
        cr.base_currency_code as from_currency_code
        ,"US Dollar" as from_currency
        ,cr.exchange_currency as to_currency_code
        ,c.currency_name as to_currency
        ,cr.exchange_rate as exchange_rate
        ,cr.loaded_at_ts
    from exchange_rates as er
    inner join currencies as c
        where er.exchange_currency = c.currency_code

)

select *
from joined
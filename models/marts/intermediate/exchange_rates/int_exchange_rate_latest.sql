with er_history as (

    select *
    from {{ ref('int_exchange_rate_history') }}
    where loaded_at_date = current_date()

), latest_value as (

    select 
        id
        ,from_currency        
        ,to_currency
        ,first_value(exchange_rate) over (
            partition by from_currency, to_currency
            order by loaded_at_ts desc
            rows between unbounded preceding and unbounded following
        ) as exchange_rate_latest
        ,max(loaded_at_ts) over (
            partition by from_currency, to_currency
            order by loaded_at_ts desc
            rows between unbounded preceding and unbounded following
        ) as loaded_at_ts_latest
    from er_history

)

select *
from latest_value

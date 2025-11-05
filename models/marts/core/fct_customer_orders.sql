select
    c.*
    ,o.status_code
from {{ ref('dim_customers') }} c
join {{ref('fct_orders')}} o
    on c.customer_key = o.customer_key
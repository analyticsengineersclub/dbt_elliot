with 

customers as (

    select * from {{ ref ('customers_macro')}}

)

select 
    date_trunc(first_order_at, week) as first_order_week,
    count(distinct customer_id) as new_customers
from customers 
group by 1
order by 1
with customers as (

    select * from {{ ref ('customers')}}

)

select 
    date_trunc(first_order_at, month) as first_order_month,
    count(distinct customer_id) as new_customers
from customers 
group by 1
order by 1
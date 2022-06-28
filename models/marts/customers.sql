with 

orders as (
  select * from {{ref ('stg_orders')}}
),

customers as (
  select * from {{ref ('stg_customers')}}
),

customer_orders as (
    select
          customer_id,
          count(*) as number_of_orders,
          min(created_at) as first_order_at
    from orders
    group by 1
)

select 
     customers.customer_id,
     customers.name,
     customers.email,
     coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
     customer_orders.first_order_at,
from customers
left join  customer_orders using(customer_id)
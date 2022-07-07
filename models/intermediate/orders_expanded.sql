with 

orders as (
    select * from {{ref ('stg_orders')}}
),

customers as (
    select * from {{ref ('customers_macro')}}
),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.total,
        orders.address,
        orders.state,
        orders.zip,
        orders.is_new_customer,
        orders.created_at,
        orders.created_at_week_date,
        customers.first_order_at,
        cast(date_trunc(customers.first_order_at, week) as date) as first_order_week_date
    from orders
    left join customers using(customer_id)

)

select * from final
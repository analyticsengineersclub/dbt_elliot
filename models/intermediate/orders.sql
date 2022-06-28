with 

orders as (
    select * from {{ref ('stg_orders')}}
),

order_items as (
    select * from {{ref ('stg_order_items')}}
),

products as (
    select * from {{ref ('stg_products')}}
),

product_prices as (
    select * from {{ref ('stg_product_prices')}}
),

customers as (
    select * from {{ref ('stg_customers')}}
),



final as (
    select 
        order_id,
        customer_id,
        product_id,
        product_category,
        amount_paid,
        amount_refunded,
        net_amount,
        is_new_customer,
        delivery_state,
        placed_at,
        returned_at,
        refunded_at
)



with 

order_items as (
    select * from {{ref ('stg_order_items')}}
),

orders as (
    select * from {{ref ('stg_orders')}}
),

products as (
    select * from {{ref ('stg_products')}}
),

prices as (
    select * from {{ref ('price_at_time_of_order')}}
),

final as (

    select 
        order_items.order_item_id,
        order_items.order_id,
        order_items.product_id,
        orders.customer_id,
        orders.state as delivery_state,
        products.product_name,
        products.product_category,
        prices.price_at_time_of_order as price,
        orders.is_new_customer,
        orders.created_at
    from order_items
    left join orders using(order_id)
    left join products using(product_id)
    left join prices using(order_item_id)

)

select * from final
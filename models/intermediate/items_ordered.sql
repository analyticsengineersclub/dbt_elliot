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

product_prices as (
    select * from {{ref ('stg_product_prices')}}
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
        product_prices.price
    from order_items
    left join orders using(order_id)
    left join products using(product_id)
    left join product_prices using(product_id)

)

select * from final
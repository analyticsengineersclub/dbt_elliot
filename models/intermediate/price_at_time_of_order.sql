with

orders as (
    select * from {{ref ('stg_orders')}}
),

order_items as (
    select * from {{ref ('stg_order_items')}}
),

product_prices as (
    select * from {{ref ('stg_product_prices')}}
),

base as (

    select
        order_items.order_item_id,
        orders.created_at as order_date,
        product_prices.price,
        product_prices.created_at as price_start_at,
        product_prices.ended_at as price_end_at,
        product_prices.has_not_ended
    from order_items
    left join product_prices using(product_id)
    left join orders using(order_id)

),

case_when as (

    select *,
        case 
            when order_date > price_start_at and has_not_ended
                then price
            when order_date between price_start_at and price_end_at
                then price
            when cast(order_date as date) = cast('2021-05-31' as date) and cast(price_end_at as date) = cast('2021-05-31' as date)
                then price
            else null end as price_at_time_of_order
    from base

),

final as (

    select 
        order_item_id,
        price_at_time_of_order
    from case_when
    where price_at_time_of_order is not null

)

select * from final
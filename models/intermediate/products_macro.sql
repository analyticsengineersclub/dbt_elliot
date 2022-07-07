with 

items_ordered as (
    select * from {{ref ('items_ordered')}}
),

products as (
    select * from {{ref ('stg_products')}}
),

aggregates as (

    select 
        product_id,
        count(distinct order_item_id) as total_purchased,
        sum(price) as total_revenue
    from items_ordered
    group by 1

),

final as (

    select 
        products.product_id,
        products.product_name,
        products.product_category,
        aggregates.total_purchased,
        aggregates.total_revenue,
        products.created_at
    from products
    left join aggregates using(product_id)

)

select * from final
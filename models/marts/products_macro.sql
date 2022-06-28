with 

items_ordered as (
    select * from {{ref ('items_ordered')}}
),

products as (
    select * from {{ref ('stg_products')}}
),

final as (

    select 
        items_ordered.product_id,
        items_ordered.product_name,
        items_ordered.product_category,
        items_ordered.price,
        count(distinct items_ordered.order_item_id) as total_purchased,
        sum(items_ordered.price) as total_revenue,
        products.created_at
    from items_ordered
    left join products using(product_id)
    group by 1,2,3,4,7
    order by 1,2,3,4,7

)

select * from final
with 

items_ordered as (
    select * from {{ref ('items_ordered')}}
),

final as (
    
    select 
       product_id,
       product_name,
       product_category,
       delivery_state,
       count(distinct order_item_id) as total_purchased,
       sum(price) as total_revenue
    from items_ordered
    group by 1,2,3,4
    order by 1,2,3,4

)

select * from final
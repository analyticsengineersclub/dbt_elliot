with 

items_ordered as (
    select * from {{ref ('items_ordered')}}
),

final as (

    select
        date_trunc(created_at, week) as date_week,
        is_new_customer,
        sum(price) as total_revenue,
        count(distinct order_id) as total_orders,
        count(distinct order_item_id) as total_items_purchased,
        count(distinct customer_id) as unique_customers
    from items_ordered
    group by 1,2
    order by 1,2

)

select * from final
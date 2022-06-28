with 

items_ordered as (
    select * from {{ref ('items_ordered')}}
),

final as (

    select 
        date_trunc(created_at, week) as date_week,
        product_category,
        sum(price) as total_revenue
    from items_ordered
    group by 1,2
    order by 1,2

)

select * from final
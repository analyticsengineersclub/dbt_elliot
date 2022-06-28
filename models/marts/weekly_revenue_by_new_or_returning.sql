with 

orders as (
    select * from {{ref ('stg_orders')}}
),

final as (

    select
        date_trunc(created_at) as date_week,
        is_new_customer,
        sum(total) as total_revenue
    from orders
    group by 1,2
    order by 1,2

)

select * from final
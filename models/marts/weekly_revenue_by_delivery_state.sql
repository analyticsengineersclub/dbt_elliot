with 

orders as (
    select * from {{ref ('stg_orders')}}
),

final as (

    select
        date_trunc(created_at, week) as date_week,
        state as delivery_state,
        sum(total) as total_revenue
    from orders
    group by 1,2
    order by 1,2
    
)

select * from final
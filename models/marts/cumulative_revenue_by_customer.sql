with 

orders as (
    select * from {{ref ('orders_expanded')}}
),

customers as (
    select * from {{ref ('customers_macro')}}
), 

dates as (
    select * from {{ref ('stg_date_dim')}}
),

week_dates as (

    select 
        dates.date as week_date, 
        customers.customer_id,
        case
            when cast(customers.first_order_at as date) >= dates.date
                then cast(
                        date_diff(
                            date_trunc(
                                cast(
                                    customers.first_order_at 
                                as date)
                            ,week)
                        , dates.date, day)
                    /7 as int64) + 1 
            else null end as customer_week_num
    from dates
    cross join customers
    where weekday_name = 'Sunday'

),

weekly_sums as (

    select 
        customer_id, 
        created_at_week_date,
        count(distinct order_id) as num_orders,
        sum(total) as weekly_revenue
    from orders
    group by 1,2

),

final as (
  
    select 
        concat(customer_id,concat('_', customer_week_num)) as customer_week_id,
        week_dates.customer_id,
        week_dates.customer_week_num,
        coalesce(weekly_sums.num_orders,0) as num_orders,
        coalesce(weekly_sums.weekly_revenue,0) as weekly_revenue,
        sum(weekly_revenue) over (
            partition by week_dates.customer_id
            order by week_dates.customer_week_num
            rows between unbounded preceding and current row
        ) as cumulative_revenue
    from week_dates
    left join weekly_sums 
        on week_dates.customer_id = weekly_sums.customer_id
        and week_dates.week_date = weekly_sums.created_at_week_date
    where week_dates.customer_week_num is not null
    order by 1,2
)

select *
from final 
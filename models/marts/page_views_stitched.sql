with 

page_views as (
    select * from {{ref ('stg_page_views')}}
),

deduped_ids as (
  select distinct 
    customer_id, 
    visitor_id,
    min(visited_at) as earliest_visit_at
  from page_views
  where customer_id is not null
  group by 1,2
),

grouped as (
  select 
    customer_id,
    string_agg(visitor_id, '_' order by earliest_visit_at asc) as visitor_id
  from deduped_ids
  group by 1
),

final as (
  select 
    page_views.page_visit_id,
    grouped.visitor_id,
    page_views.customer_id,
    page_views.page_visited,
    page_views.device_type,
    page_views.visited_at
  from page_views
  left join grouped using(customer_id)
)

select * from final
with 

page_visits as (
    select * from {{ref ('stg_page_visits')}} where customer_id is not null
),

deduped_ids as (
  select distinct 
    customer_id, 
    visitor_id,
    min(visited_at) as earliest_visit_at
  from page_visits
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
    page_visits.page_visit_id,
    grouped.visitor_id
  from page_visits
  left join grouped using(customer_id)
)

select * from final
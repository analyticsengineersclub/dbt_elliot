with 

page_visits as (
    select * from {{ref ('stg_page_visits')}}
),

prev_visit_at as (

  select 
    page_visit_id,
    visitor_id,
    device_type,
    visited_at,
    lag(visited_at) 
      over (
          partition by visitor_id, device_type
          order by visited_at asc
      ) as previous_visit_at
  from page_visits

),

minutes_since as (

  select *,
    timestamp_diff(visited_at, previous_visit_at, minute) as minutes_since_prev_visit
  from prev_visit_at

),

root_of_session as (

  select *,
    case 
      when minutes_since_prev_visit > 30 or minutes_since_prev_visit is null 
        then page_visit_id
      else null end as root_visit
  from minutes_since

),

session_ids as (

  select 
    page_visit_id,
    visited_at,
    last_value(root_visit ignore nulls) 
      over (
        partition by visitor_id, device_type 
        order by visited_at asc
        range between unbounded preceding and current row
        ) as session_id
  from root_of_session

),

action_rank as (

  select 
    page_visit_id,
    session_id,
    row_number() over (partition by session_id order by visited_at asc) as session_action_rank,
  from session_ids

),

session_agg as (

  select 
    session_id, 
    max(session_action_rank) as num_actions_in_session
  from action_rank
  group by 1

),


final as (
  
  select 
    action_rank.page_visit_id,
    action_rank.session_id,
    action_rank.session_action_rank,
    session_agg.num_actions_in_session
  from action_rank
  left join session_agg using(session_id)

)

select * from final


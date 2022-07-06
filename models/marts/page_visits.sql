with

visits as (
    select * from {{ref ('stg_page_visits')}}
),

visitors as (
    select * from {{ref ('page_visits_stitched')}}
),

visitor_sessions as (
    select * from {{ref ('page_visits_w_session')}}
),

final as (

    select 
        visits.page_visit_id,
        visitors.visitor_id,
        visits.customer_id,
        visitor_sessions.session_id,
        visitor_sessions.session_action_rank,
        visitor_sessions.session_action_rank = 1 as is_first_session_action,
        visitor_sessions.session_action_rank = visitor_sessions.num_actions_in_session as is_last_session_action,
        visits.page_visited,
        visits.device_type,
        visits.visited_at
    from visits
    left join visitors using(page_visit_id)
    left join visitor_sessions using(page_visit_id)

)

select * from final
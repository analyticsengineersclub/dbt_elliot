with source as (

    select * from {{ source('web_tracking', 'pageviews') }}

),

renamed as (

    select
        id as page_visit_id,
        visitor_id,
        customer_id,
        page as page_visited,
        device_type,
        timestamp as visited_at

    from source

)

select * from renamed
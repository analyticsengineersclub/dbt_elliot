with source as (

    select * from {{ source('coffee_shop', 'orders') }}

),

base as (

    select
        source.*,
        row_number() over (partition by customer_id order by created_at asc) as row
    from source

),

renamed as (
    select
        id as order_id,
        customer_id,
        total,
        address,
        state,
        zip,
        created_at,
        row=1 as is_new_customer
    from base
)

select * from renamed
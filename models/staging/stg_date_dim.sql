with final as (

    select
        format_date('%F', d) as id,
        d as date,
        extract(year from d)     as year,
        extract(week from d)     as week_in_year,
        extract(day from d)      as year_day,
        extract(year from d)     as fiscal_year,
        format_date('%Q', d)     as fiscal_qtr,
        extract(month from d)    as month_number,
        format_date('%B', d)     as month_name,
        format_date('%w', d)     as weekday_number,
        format_date('%A', d)     as weekday_name,
        case 
            when format_date('%A', d) in ('Saturday','Sunday') 
                then 'Weekend' 
            else 'Weekday' end as weekday_weekend
    from (
        select * 
        from unnest(generate_date_array('2020-12-01','2021-12-31',interval 1 day)) as d
        )
)

select * from final
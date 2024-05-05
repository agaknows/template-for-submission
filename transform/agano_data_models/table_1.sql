{{
    config(
        materialized='table'
    )
}}

with
    ord_1 as (
        select
            row_number() over (order by date_local asc) as order_index,
            rdbms_id,
            country_name,
            date_local,
            vendor_id,
            customer_id,
            gmv_local,
            is_voucher_used,
            is_successful_order,
            product_id
        from `foodpanda-422109.cs_1_2.orders`
    ),

    ord_2 as (
        select
            order_index,
            rdbms_id,
            country_name,
            date_local,
            vendor_id,
            customer_id,
            gmv_local,
            is_voucher_used,
            is_successful_order,
            trim(product_id) as product_id
        from ord_1, unnest(split(product_id, ',')) as product_id
    ),

    ord_3 as (
        select *, count(*) as product_quantity
        from ord_2
        group by
            order_index,
            rdbms_id,
            country_name,
            date_local,
            vendor_id,
            customer_id,
            gmv_local,
            is_voucher_used,
            is_successful_order,
            product_id
    ),

    final_table as (
        select *
        from ord_3 o
        left join `foodpanda-422109.cs_1_2.vendors` v on v.id = o.vendor_id
        left join
            `foodpanda-422109.cs_1_2.products` p on cast(p.id as string) = o.product_id
        left join `foodpanda-422109.cs_1_2.customers` c on c.id = o.customer_id
    ),

    successful_orders_per_day as (
        select date_local, count(distinct order_index) as successful_orders
        from final_table
        where is_successful_order = true
        group by date_local
    )

select *
from successful_orders_per_day

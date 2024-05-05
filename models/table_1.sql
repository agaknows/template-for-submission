{{
    config(
        materialized='view'
    )
}}

-- models/order_analysis.sql

with ord_1 as (
    SELECT 
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
    FROM `foodpanda-422109.cs_1_2.orders`
),

ord_2 as (
    SELECT 
        order_index,
        rdbms_id,
        country_name,
        date_local,
        vendor_id,
        customer_id,
        gmv_local,
        is_voucher_used,
        is_successful_order,
        TRIM(product_id) AS product_id
    FROM ord_1,
    UNNEST(SPLIT(product_id, ',')) AS product_id
),

ord_3 as (
    SELECT 
        *,
        COUNT(*) as product_quantity
    FROM ord_2
    GROUP BY 
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
    SELECT *
    FROM ord_3 o
    LEFT JOIN `foodpanda-422109.cs_1_2.vendors` v ON v.id = o.vendor_id
    LEFT JOIN `foodpanda-422109.cs_1_2.products` p ON cast(p.id as string) = o.product_id
    LEFT JOIN `foodpanda-422109.cs_1_2.customers` c ON c.id = o.customer_id
),

successful_orders_per_day as (
    SELECT date_local, COUNT(DISTINCT order_index) as successful_orders
    FROM final_table 
    WHERE is_successful_order=true
    GROUP BY date_local
)

SELECT * FROM successful_orders_per_day

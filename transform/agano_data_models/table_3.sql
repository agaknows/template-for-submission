{{
    config(
        materialized='table'
    )
}}


-- This CTE assumes that each row in the orders dataset corresponds to one transaction. The added field order_index acts as an id for each transaction.
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

-- This CTE separates the product_ids listed in each cell of the product_id column and creates a separate row for each. This allows us to connect the orders table to the products table.
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

-- This CTE counts the quantity of products ordered in each transaction
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

-- This table joins ALL of the given tables into one
final_table as (
    SELECT *
    FROM ord_3 o
    LEFT JOIN `foodpanda-422109.cs_1_2.vendors` v ON v.id = o.vendor_id
    LEFT JOIN `foodpanda-422109.cs_1_2.products` p ON cast(p.id as string) = o.product_id
    LEFT JOIN `foodpanda-422109.cs_1_2.customers` c ON c.id = o.customer_id
)

-- This counts the number of successful orders per restaurant per day
SELECT vendor_name, date_local, COUNT(DISTINCT order_index) as order_count
FROM final_table 
WHERE is_successful_order= true
GROUP BY vendor_name, date_local
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
),

-- This CTE calculates the number of days elapsed from the maximum date in the dataset for each transaction
last_7_days as (
    SELECT 
        *,
        DATE_DIFF(MAX(date_local) OVER(), date_local, DAY) as date_diff_from_max
    FROM final_table
),

-- This CTE calculates the number of orders made by each customer in the last 7 days
reorders7 as (
    SELECT 
        customer_id, 
        COUNT(DISTINCT order_index) AS orders_last_7_days
    FROM last_7_days
    WHERE date_diff_from_max <= 7
    GROUP BY customer_id
)

-- This query counts the number of distinct customers who reordered within the last 7 days
SELECT COUNT (DISTINCT customer_id) as customers_reordered
FROM reorders7
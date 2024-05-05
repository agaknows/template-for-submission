{{
    config(
        materialized='view'
    )
}}

WITH customers AS {
    SELECT
      customer_id,
      ...
}
  {{ config(
    materialized='table'
) }}

WITH final_data AS (
    SELECT 
        country, 
        COUNT(*) AS total_customers 
    FROM {{ source ('dev_layer','dim_customer')}}
    GROUP BY country
)

SELECT * FROM final_data

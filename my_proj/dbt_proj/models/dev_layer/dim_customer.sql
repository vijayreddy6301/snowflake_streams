{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('RAW_LAYER', 'CUSTOMER') }}
    {% if is_incremental() %}
    WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
    {% endif %}
),  

existing_data AS (
    SELECT * FROM {{ this }}
),

changed_records AS (
    SELECT 
        s.index,
        s.customer_id,
        s.first_name,
        s.last_name,
        s.company,
        s.city,
        s.country,
        s.phone_1,
        s.phone_2,
        s.email,
        s.subscription_date,
        s.web_site,
        s.last_updated_at  -- Ensure timestamp updates on change
    FROM {{ source('RAW_LAYER', 'CUSTOMER') }} s
    INNER JOIN existing_data e
    ON s.customer_id = e.customer_id
    WHERE 
        s.company <> e.company 
        OR s.city <> e.city 
        OR s.country <> e.country 
        OR s.email <> e.email
)

SELECT * FROM source_data
UNION ALL
SELECT * FROM changed_records

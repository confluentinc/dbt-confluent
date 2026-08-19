{{ config(
    materialized='materialized_table'
) }}

SELECT city, COUNT(*) AS user_count
FROM {{ ref('users') }}
GROUP BY city

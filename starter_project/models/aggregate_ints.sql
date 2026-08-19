{{ config(materialized='materialized_table') }}

SELECT 1 as id, SUM(val) AS total_val
FROM {{ ref('static_ints') }}

{{ config(
    materialized='materialized_table',
    start_mode='FROM_BEGINNING'
  )
}}

SELECT 1 as id, SUM(val) AS total_val
FROM {{ ref('source_values') }}

{{ config(
    materialized='table',
    tableflow={
        'table_formats': ['ICEBERG'],
        'storage': {'kind': 'Managed'},
        'config': {
          'error_handling': { 'mode': 'SKIP' }
        }
    }
) }}

select 1 as order_id, 'widget' as item, 9.99 as price

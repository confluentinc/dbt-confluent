{{ config(
    materialized='table',
    contract={'enforced': true},
    distributed_by={'columns': ['id'], 'buckets': 1}
) }}

SELECT * FROM (VALUES
  (1, 1),
  (2, 2),
  (3, 3),
  (4, 4),
  (5, 5),
  (6, 6),
  (7, 7),
  (8, 8),
  (9, 9),
  (10, 10)
) AS t(id, val)

Upgraded the `confluent-sql` driver to `0.4.0`, unlocking two new credential options:

- **Global API keys**: authenticate with `global_api_key` / `global_api_secret` (a "Global" Confluent Cloud key that works against every route) as an alternative to the Flink-region `flink_api_key` / `flink_api_secret` pair. Supply exactly one complete pair; the Global pair is preferred when both are given.
- **Poolless Flink**: `compute_pool_id` is now optional. When omitted, statements run in the environment+region default compute pool (provisioned if necessary), instead of requiring an explicit pool.

`dbt init` prompts for both choices, and `dbt debug` now reports the configured compute pool.

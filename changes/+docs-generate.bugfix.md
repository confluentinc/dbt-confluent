`dbt docs generate` now only reads catalog metadata for the project's own relations, in small retried chunks, instead of querying the whole environment's `INFORMATION_SCHEMA`, which often timed out.

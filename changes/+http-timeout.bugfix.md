Increase the HTTP client timeout to 60s so cold INFORMATION_SCHEMA lookups (notably the unified drift-catalog UNION ALL) no longer surface as "read operation timed out" on the default 5s budget.

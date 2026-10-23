Add `SingleFlightCell` and `SingleFlightDict`, small helpers that compute a value once across dbt worker threads, as groundwork for sharing one UDF artifact between several functions.

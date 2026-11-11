"""How Flink reports WITH option values written different ways, as (option, written, reported)
triples from the GH-121 probe runs.

The option_values unit tests check option_values_match against these, and a functional test
(test_schema_drift.py) creates tables with them and checks Flink still reports them this way. So if
Flink changes, that test fails, instead of the unit tests passing with stale values. When it does,
update the triples here, then check option_values_match still handles the new forms.

Every duration and size unit spelling option_values accepts appears at least once. The functional
test packs entries into tables by position (its first table sets each option's first entry, and so
on), so the option with the most entries sets the number of tables; spread new entries across
options to keep it near 18.
"""

FLINK_REPORTED_VALUES = [
    # Durations
    ("kafka.retention.time", "7 days", "7 d"),
    ("kafka.retention.time", "7d", "7 d"),
    ("kafka.retention.time", "7 D", "7 d"),
    ("kafka.retention.time", "7 day", "7 d"),
    ("kafka.retention.time", "168 h", "7 d"),
    ("kafka.retention.time", "36 hour", "36 h"),
    ("kafka.retention.time", "36 hours", "36 h"),
    ("kafka.retention.time", "10080 min", "7 d"),
    ("kafka.retention.time", "90 m", "90 min"),
    ("kafka.retention.time", "90 minute", "90 min"),
    ("kafka.retention.time", "90 minutes", "90 min"),
    ("kafka.retention.time", "0", "0 ms"),
    ("kafka.compaction.time", "90 s", "90 s"),
    ("kafka.compaction.time", "90 sec", "90 s"),
    ("kafka.compaction.time", "90 secs", "90 s"),
    ("kafka.compaction.time", "90 second", "90 s"),
    ("kafka.compaction.time", "90 seconds", "90 s"),
    ("kafka.compaction.time", "1500 ms", "1500 ms"),
    ("kafka.compaction.time", "1500 milli", "1500 ms"),
    ("kafka.compaction.time", "1500 millis", "1500 ms"),
    ("kafka.compaction.time", "1500 millisecond", "1500 ms"),
    ("kafka.compaction.time", "1500 milliseconds", "1500 ms"),
    ("kafka.compaction.time", "604800000", "7 d"),
    # Below a millisecond: Flink reports whole milliseconds, rounded down
    ("kafka.retention.time", "1000000000 ns", "1 s"),
    ("kafka.retention.time", "90000000000 nano", "90 s"),
    ("kafka.retention.time", "1000 µs", "1 ms"),
    ("kafka.retention.time", "90000000 micro", "90 s"),
    ("kafka.retention.time", "1500 µs", "1 ms"),
    ("kafka.retention.time", "1 ns", "0 ms"),
    ("kafka.compaction.time", "90000000000 nanos", "90 s"),
    ("kafka.compaction.time", "90000000000 nanosecond", "90 s"),
    ("kafka.compaction.time", "90000000000 nanoseconds", "90 s"),
    ("kafka.compaction.time", "90000000 micros", "90 s"),
    ("kafka.compaction.time", "90000000 microsecond", "90 s"),
    ("kafka.compaction.time", "90000000 microseconds", "90 s"),
    # Sizes
    ("kafka.retention.size", "1 gb", "1 gb"),
    ("kafka.retention.size", "1gb", "1 gb"),
    ("kafka.retention.size", "1 GB", "1 gb"),
    ("kafka.retention.size", "1073741824", "1 gb"),
    ("kafka.retention.size", "1073741824 bytes", "1 gb"),
    ("kafka.retention.size", "1048577 kb", "1048577 kb"),
    ("kafka.retention.size", "2 g", "2 gb"),
    ("kafka.retention.size", "2 gibibytes", "2 gb"),
    ("kafka.retention.size", "1 t", "1099511627776 bytes"),
    ("kafka.retention.size", "1 tb", "1099511627776 bytes"),
    ("kafka.retention.size", "1 tebibytes", "1099511627776 bytes"),
    ("kafka.retention.size", "1536 mebibytes", "1536 mb"),
    ("kafka.max-message-size", "1048576", "1 mb"),
    ("kafka.max-message-size", "1 mb", "1 mb"),
    ("kafka.max-message-size", "1048576 b", "1 mb"),
    ("kafka.max-message-size", "1024 k", "1 mb"),
    ("kafka.max-message-size", "1024 kibibytes", "1 mb"),
    ("kafka.max-message-size", "1 m", "1 mb"),
    # Enum values Flink lowercases
    ("kafka.cleanup-policy", "COMPACT", "compact"),
    ("kafka.cleanup-policy", "Delete-Compact", "delete-compact"),
    ("kafka.message-timestamp-type", "LOG-APPEND-TIME", "log-append-time"),
    # Enum values Flink keeps as written
    ("changelog.mode", "Retract", "Retract"),
    ("scan.startup.mode", "LATEST-OFFSET", "LATEST-OFFSET"),
    ("kafka.producer.compression.type", "LZ4", "LZ4"),
    ("kafka.consumer.isolation-level", "READ-COMMITTED", "READ-COMMITTED"),
]

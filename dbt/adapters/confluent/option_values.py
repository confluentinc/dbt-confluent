"""How the drift check compares a WITH option value the model sets with the one the table has.

The drift check reads the existing table's WITH options from INFORMATION_SCHEMA.TABLE_OPTIONS,
which doesn't always report a value the way the model wrote it. Flink shows a duration or a size
in the largest unit it divides into evenly ('7 days' and '604800000' both become '7 d', '1024 mb'
becomes '1 gb'), and lowercases some enum values ('COMPACT' becomes 'compact'). Comparing the
strings would report drift on every run for a model that hasn't changed.

`option_values_match` compares them by meaning instead. It needs no list of options: a duration or
size is told apart by the form Flink reports it in, so it works for any option, connector or
format.
"""

# Flink's duration units (TimeUtils.parseDuration), in milliseconds. µs and ns are left out: no
# Kafka-backed option goes below milliseconds, so a value written in them is reported as drift.
_DURATION_UNITS_MS = {
    **dict.fromkeys(("ms", "milli", "millis", "millisecond", "milliseconds"), 1),
    **dict.fromkeys(("s", "sec", "secs", "second", "seconds"), 1000),
    **dict.fromkeys(("min", "m", "minute", "minutes"), 60_000),
    **dict.fromkeys(("h", "hour", "hours"), 3_600_000),
    **dict.fromkeys(("d", "day", "days"), 86_400_000),
}

# Flink's memory size units (MemorySize.parse), in bytes.
_SIZE_UNITS_BYTES = {
    **dict.fromkeys(("b", "bytes"), 1),
    **dict.fromkeys(("k", "kb", "kibibytes"), 1024),
    **dict.fromkeys(("m", "mb", "mebibytes"), 1024**2),
    **dict.fromkeys(("g", "gb", "gibibytes"), 1024**3),
    **dict.fromkeys(("t", "tb", "tebibytes"), 1024**4),
}

# The units TABLE_OPTIONS reports a duration or a size in: a whole number, one space, then the
# largest of these it divides into evenly ('7 d', '90 min', '1 gb', '2097164 bytes'). Sizes stop at
# gb (1 TiB is reported in bytes). The two sets don't overlap, so the reported value says which
# kind of value the option holds.
_REPORTED_DURATION_UNITS = frozenset({"ms", "s", "min", "h", "d"})
_REPORTED_SIZE_UNITS = frozenset({"bytes", "kb", "mb", "gb"})


def option_values_match(expected: str, existing: str) -> bool:
    """Return whether a WITH option value the model sets means the same as the table's.

    expected is the model's value as a string; existing is the value TABLE_OPTIONS reports. Equal
    strings match. If existing is a duration or a size in the form Flink reports one, both are read
    as a number of milliseconds or bytes, the way Flink reads them (a bare number is milliseconds
    or bytes), and match if the numbers are equal; a model value that can't be read that way
    doesn't match. Any other value matches if it's equal ignoring case.

    Ignoring case means a change to a free-text value that only alters case (a subject name, a
    faker expression) isn't reported as drift. Flink reads enum values without regard to case, so
    for those a change in case changes nothing.
    """

    def amount(value: str, units: dict[str, int]) -> int | None:
        """Read value the way Flink does: a whole number, then an optional unit (bare is 1).

        Anything malformed ends up in the unit, which the lookup in units rejects.
        """
        value = value.strip()
        unit = value.lstrip("0123456789")
        number = value[: len(value) - len(unit)]
        if not number:
            return None
        unit = unit.strip().lower()
        factor = units.get(unit) if unit else 1
        return None if factor is None else int(number) * factor

    if expected == existing:
        return True
    number, _, unit = existing.partition(" ")
    if number and not number.lstrip("0123456789"):
        if unit in _REPORTED_DURATION_UNITS:
            return amount(expected, _DURATION_UNITS_MS) == amount(existing, _DURATION_UNITS_MS)
        if unit in _REPORTED_SIZE_UNITS:
            return amount(expected, _SIZE_UNITS_BYTES) == amount(existing, _SIZE_UNITS_BYTES)
    return expected.casefold() == existing.casefold()

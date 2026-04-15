import hashlib
import re

MAX_STATEMENT_NAME_LENGTH = 100
_ILLEGAL_CHARS_RE = re.compile(r"[^a-z0-9-]")


def sanitize_statement_name(name: str) -> str:
    """Sanitize a Flink statement name.

    Flink statement names must:
    - Contain only lowercase alphanumeric characters and hyphens
    - Start with an alphanumeric character
    - Be at most 100 characters long

    This function:
    - Lowercases the input
    - Replaces characters not in [a-z0-9-] (including underscores) with '-'
    - If any replacement occurred, appends a 6-char hash of the original
      to avoid collisions (e.g. 'a_b' and 'a.b' won't clash)
    - Strips leading hyphens (name must start with alphanumeric)
    - Truncates to 100 chars with a 6-char hash suffix if needed
    """
    if not name:
        raise ValueError("Statement name cannot be empty")
    original = name
    name = name.lower()
    sanitized = _ILLEGAL_CHARS_RE.sub("-", name)

    if sanitized != name:
        short_hash = hashlib.md5(original.encode()).hexdigest()[:6]
        sanitized = f"{sanitized}-{short_hash}"

    # Strip leading hyphens — name must start with alphanumeric
    sanitized = sanitized.lstrip("-")

    if len(sanitized) > MAX_STATEMENT_NAME_LENGTH:
        hash_suffix = hashlib.md5(original.encode()).hexdigest()[:6]
        max_base = MAX_STATEMENT_NAME_LENGTH - 1 - len(hash_suffix)  # 1 for '-'
        sanitized = f"{sanitized[:max_base]}-{hash_suffix}"

    return sanitized

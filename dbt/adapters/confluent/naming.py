import hashlib
import re

MAX_STATEMENT_NAME_LENGTH = 72
_VALID_CHARS_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_ILLEGAL_CHARS_RE = re.compile(r"[^a-zA-Z0-9_-]")


def sanitize_statement_name(name: str) -> str:
    """Sanitize a Flink statement name.

    - Replaces characters not in [a-zA-Z0-9_-] with '_'
    - If any replacement occurred, appends a 4-char hash of the original
      to avoid collisions (e.g. 'a.b' and 'a_b' won't clash)
    - Truncates to 72 chars with a 6-char hash suffix if needed
    """
    sanitized = _ILLEGAL_CHARS_RE.sub("_", name)
    if sanitized != name:
        short_hash = hashlib.md5(name.encode()).hexdigest()[:4]
        sanitized = f"{sanitized}-{short_hash}"

    if len(sanitized) > MAX_STATEMENT_NAME_LENGTH:
        hash_suffix = hashlib.md5(name.encode()).hexdigest()[:6]
        max_base = MAX_STATEMENT_NAME_LENGTH - 1 - len(hash_suffix)  # 1 for '-'
        sanitized = f"{sanitized[:max_base]}-{hash_suffix}"

    return sanitized


def validate_statement_name(name: str) -> None:
    """Validate a statement name against Flink constraints.

    Raises ValueError if the name is invalid.
    """
    if not name:
        raise ValueError("Statement name cannot be empty")
    if len(name) > MAX_STATEMENT_NAME_LENGTH:
        raise ValueError(
            f"Statement name exceeds {MAX_STATEMENT_NAME_LENGTH} char limit: '{name}'"
        )
    if not _VALID_CHARS_RE.match(name):
        raise ValueError(f"Statement name contains illegal characters: '{name}'")

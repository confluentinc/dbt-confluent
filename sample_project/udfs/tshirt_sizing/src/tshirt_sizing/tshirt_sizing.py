# pyright: reportMissingImports=false
# (pyflink is only available in the Confluent Cloud UDF runtime, not this project's environment)
from pyflink.table import DataTypes
from pyflink.table.types import DataType
from pyflink.table.udf import udf

# Dictionary mapping both full names and abbreviations to numeric size values
_SIZE_MAP = {
    "x-small": 0,
    "xs": 0,
    "small": 1,
    "s": 1,
    "medium": 2,
    "m": 2,
    "large": 3,
    "l": 3,
    "x-large": 4,
    "xl": 4,
    "xx-large": 5,
    "xxl": 5,
}


def _get_size_value(shirt: str) -> int:
    """
    Returns the numeric size value for a given shirt size string.
    Returns -1 if the size is not found.
    """
    if shirt is None:
        return -1
    return _SIZE_MAP.get(shirt.strip().lower(), -1)


def _f_is_smaller(shirt1: str, shirt2: str) -> bool:
    """
    Returns True if shirt1 is a smaller size than shirt2 based on standard T-shirt sizes.
    If a size cannot be found, returns False.
    """
    size1 = _get_size_value(shirt1)
    size2 = _get_size_value(shirt2)
    if size1 == -1 or size2 == -1:
        return False
    return size1 < size2


_is_smaller_inp_types: list[DataType] = [
    DataTypes.STRING(),
    DataTypes.STRING(),
]
is_smaller = udf(
    _f_is_smaller,
    input_types=_is_smaller_inp_types,
    result_type=DataTypes.BOOLEAN(),
)

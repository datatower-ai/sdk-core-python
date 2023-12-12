from enum import Enum


class ExceedInsertionStrategy(Enum):
    """The insertion strategy used when the size is reaching upper bound."""

    """Delete n oldest data where n is number of data exceeded (lose oldest data)"""
    DELETE = 1

    """Stop at the boundary and discard exceeded tail data (lose latest data)"""
    DISCARD = 2

    """Stop at the boundary and discard exceeded head data (lose latest data)"""
    DISCARD_HEAD = 3

    """Abort the insertion and discard current data (lose latest data)"""
    ABORT = 4

    """Ignore the upper bound limits and insert data (unsafe)"""
    IGNORE = 5

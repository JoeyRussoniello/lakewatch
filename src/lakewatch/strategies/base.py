"""The Generic Strategy class that is implemented for each strategy"""

from abc import ABC, abstractmethod

from pyspark.sql import Column, DataFrame

from lakewatch.core.result import TestResult


# TODO: Document the interaction between TestStrategy TestBuilder and TestResult
class TestStrategy(ABC):
    """Abstract base class for all test strategies"""

    @abstractmethod
    def execute(self, df: DataFrame, table_name: str, **kwargs) -> TestResult:
        """Execute the test and return a TestResult"""
        pass

    @property
    @abstractmethod
    def test_name(self) -> str:
        "Returns the name of this test"
        pass


def require_columns(df: DataFrame, cols: list[str | Column], where: str):
    """
    Verify that all required columns exist in df.

    Args:
        df: Spark DataFrame
        cols: list of column names or Column objects
        where: string describing where the check is happening (for error context)

    Raises:
        ValueError if any required columns are missing
    """
    # Normalize str|Column into names
    normalized = []
    for c in cols:
        if isinstance(c, str):
            normalized.append(c)
        elif isinstance(c, Column):
            # Spark Column often has an internal ._jc.toString()
            # safer to let the caller alias() before passing
            name = c._jc.toString() if hasattr(c, "_jc") else str(c)
            normalized.append(name)
        else:
            raise TypeError(f"Unsupported column type {type(c)} in require_columns")

    missing = [c for c in normalized if c not in df.columns]
    if missing:
        raise ValueError(f"{where}: missing columns {missing}. Available: {df.columns}")

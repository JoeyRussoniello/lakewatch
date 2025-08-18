"""
Test Strategy for Effective Date Valditiy.
Should be used to ensure that all start dates are less than all end dates
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakewatch.core.result import TestResult, TestResultBuilder
from lakewatch.strategies.base import TestStrategy, require_columns


class EffectiveDateValidityTest(TestStrategy):
    """Test that a given start date is less than or equal to a given end date
    Used when intending to explode a sequence of date intervals"""

    def __init__(self, id_col: str, start_col: str, end_col: str):
        self.id_col = id_col
        self.start_col = start_col
        self.end_col = end_col

    @property
    def test_name(self) -> str:
        return f"Date Validity ({self.start_col} <= {self.end_col})"

    def execute(self, df: DataFrame, table_name: str, **kwargs) -> TestResult:
        """Execute an effective date validity test

        kwargs:
            sampe_mode (bool): Whether or not to use random sampling to increase performance
        """
        sample_mode = kwargs.get("sample_mode", False)
        require_columns(
            df, [self.id_col, self.start_col, self.end_col], where=self.test_name
        )

        builder = TestResultBuilder.for_test(self.test_name, table_name, sample_mode)

        try:
            total_count = df.count()
            invalid_df = df.filter(F.col(self.start_col) > F.col(self.end_col))
            error_count = invalid_df.count()

            details = f"Found {error_count} records with start_date > end_date out of {total_count} total records"

            if error_count > 0:
                print("Sample of invalid records:")
                invalid_df.select(self.id_col, self.start_col, self.end_col).show(5)

            return (
                builder.auto_pass_fail(total_count, error_count, details)
                .end_timing()
                .build()
            )

        except Exception as e:
            return (
                builder.exception(e, "Failed to validate effective dates")
                .end_timing()
                .build()
            )

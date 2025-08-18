"""
date_continuity. A testing strategy to confirm that a given date column
is sequential for each given id. To test without the id dependency, use
F.lit(1) or an equivalent as the id_col"""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from lakewatch.core.result import TestResult, TestResultBuilder
from lakewatch.strategies.base import TestStrategy, require_columns


class DateContinuityTest(TestStrategy):
    """Tests that dates are contiguous with no gaps"""

    def __init__(self, id_col: str | Column, date_col: str):
        self.id_col = id_col
        self.date_col = date_col

    @property
    def test_name(self) -> str:
        return f"Date Column Continuity {self.date_col} by {self.id_col}"

    def execute(self, df: DataFrame, table_name: str, **kwargs) -> TestResult:
        sample_mode = kwargs.get("sample_mode")
        # ID column isn't required in the df, since we may choose to group by a literal
        require_columns(df, [self.date_col], where=self.test_name)

        builder = TestResultBuilder.for_test(self.test_name, table_name, sample_mode)

        try:
            id_alias = "_id"
            id_column = (
                F.col(self.id_col) if isinstance(self.id_col, str) else self.id_col
            ).alias(id_alias)
            date_column = (
                F.col(self.date_col)
                if isinstance(self.date_col, str)
                else self.date_col
            )

            total_by_id = df.select(id_column).distinct().count()

            unit_date_ranges = df.groupBy(id_column).agg(
                F.min(date_column).alias("min_date"),
                F.max(date_column).alias("max_date"),
                F.count(date_column).alias("actual_count"),
            )

            unit_expected = unit_date_ranges.withColumn(
                "expected_count", F.datediff(F.col("max_date"), F.col("min_date")) + 1
            )

            gaps_df = unit_expected.filter(
                F.col("actual_count") < F.col("expected_count")
            )
            error_count = gaps_df.count()

            details = f"Found {error_count} {id_alias}s with date gaps out of {total_by_id} {self.id_col}s"

            if error_count > 0:
                print(f"Sample {self.id_col}s with gaps:")

            return builder.auto_build(total_by_id, error_count, details)
        except Exception as e:
            return (
                builder.exception(
                    e, f"Failed to test date continuity for {self.date_col}"
                )
                .end_timing()
                .build()
            )

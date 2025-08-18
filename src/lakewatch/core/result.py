"""Some docstring"""

from datetime import datetime
from typing import Dict, Optional

from pandas import DataFrame as pd_DataFrame
from pyspark.sql import DataFrame


class TestResult:
    """Immutable test result object"""

    def __init__(
        self,
        test_name: str,
        table_name: str,
        passed: bool,
        error_count: int = 0,
        total_count: int = 0,
        details: str = "",
        execution_time: float = 0.0,
        sample_mode: bool = False,
        timestamp: Optional[datetime] = None,
        example_failures: Optional[pd_DataFrame] = None,
    ):
        self.test_name = test_name
        self.table_name = table_name
        self.passed = passed
        self.error_count = error_count
        self.total_count = total_count
        self.success_rate = (
            (total_count - error_count) / total_count if total_count > 0 else 1.0
        )
        self.details = details
        self.execution_time_seconds = execution_time
        self.timestamp = timestamp or datetime.now()
        self.sample_mode = sample_mode
        self._example_failures = example_failures

    def to_dict(self) -> Dict:
        """Converts the TestResult to a dictionary record"""
        return {
            "test_name": self.test_name,
            "table_name": self.table_name,
            "passed": self.passed,
            "error_count": self.error_count,
            "total_count": self.total_count,
            "success_rate": self.success_rate,
            "details": self.details,
            "execution_time_seconds": self.execution_time_seconds,
            "timestamp": self.timestamp,
            "sample_mode": self.sample_mode,
            # serialize to records for Spark/JSON friendliness
            "failures": (
                self._example_failures.to_dict(orient="records")
                if self._example_failures is not None
                else None
            ),
        }


class TestResultBuilder:
    """Builder for creating TestResult objects incrementally"""

    def __init__(self):
        self._test_name: Optional[str] = None
        self._table_name: Optional[str] = None
        self._passed: Optional[bool] = None
        self._error_count: int = 0
        self._total_count: int = 0
        self._details: str = ""
        self._execution_time: float = 0.0
        self._sample_mode: bool = False
        self._start_time: Optional[datetime] = None
        self._example_failures: Optional[pd_DataFrame] = None

    def test_name(self, name: str) -> "TestResultBuilder":
        """Set the test name"""
        self._test_name = name
        return self

    def table_name(self, name: str) -> "TestResultBuilder":
        """Set the table name"""
        self._table_name = name
        return self

    def start_timing(self) -> "TestResultBuilder":
        """Start timing the test execution"""
        self._start_time = datetime.now()
        return self

    def end_timing(self) -> "TestResultBuilder":
        """End timing and calculate execution time"""
        if self._start_time:
            self._execution_time = (datetime.now() - self._start_time).total_seconds()
        return self

    def counts(self, total: int, errors: int = 0) -> "TestResultBuilder":
        """Set total and error counts"""
        self._total_count = total
        self._error_count = errors
        return self

    def passed(self, is_passed: bool) -> "TestResultBuilder":
        """Set whether the test passed"""
        self._passed = is_passed
        return self

    def details(self, details: str) -> "TestResultBuilder":
        """Set test details/description"""
        self._details = details
        return self

    def sample_mode(self, is_sample: bool) -> "TestResultBuilder":
        """Set whether test was run in sample mode"""
        self._sample_mode = is_sample
        return self

    def cache_failure(
        self,
        failures_df: DataFrame,
        limit_rows: int = 5,
        sample_cols: Optional[list[str]] = None,
        max_bytes: int = 256_000,  # ~250 KB guard
    ) -> "TestResultBuilder":
        """Materialize a tiny, driver-safe sample of failing rows as pandas."""
        df = failures_df
        if sample_cols:
            # only keep requested columns if present
            keep = [c for c in sample_cols if c in df.columns]
            if keep:
                df = df.select(*keep)
        pdf = df.limit(limit_rows).toPandas()

        # crude size guard
        if pdf.memory_usage(index=True, deep=True).sum() > max_bytes:
            # trim to first N columns to stay under the cap
            max_cols = max(1, int(len(pdf.columns) / 2))
            pdf = pdf.iloc[:, :max_cols]

        self._example_failures = pdf
        return self

    def success(self, total: int, details: str = "") -> "TestResultBuilder":
        """Convenience method for successful test"""
        return self.counts(total, 0).passed(True).details(details)

    def failure(
        self, total: int, errors: int, details: str = ""
    ) -> "TestResultBuilder":
        """Convenience method for failed test"""
        return self.counts(total, errors).passed(False).details(details)

    def exception(self, error: Exception, details: str = "") -> "TestResultBuilder":
        """Convenience method for test that threw exception"""
        error_details = (
            f"{details} Exception: {str(error)}"
            if details
            else f"Exception: {str(error)}"
        )
        return self.counts(-1, -1).passed(False).details(error_details)

    def auto_pass_fail(
        self, total: int, errors: int, details: str = ""
    ) -> "TestResultBuilder":
        """Automatically set pass/fail based on error count"""
        is_passed = errors == 0
        return self.counts(total, errors).passed(is_passed).details(details)

    def auto_build(self, total: int, errors: int, details: str = "") -> TestResult:
        """Automatically set pass/fail and build test result"""
        return self.auto_pass_fail(total, errors, details).end_timing().build()

    @classmethod
    def for_test(
        cls, test_name: str, table_name: str, sample_mode: bool = False
    ) -> "TestResultBuilder":
        """Factory method to create a builder with common initial setup"""
        return (
            cls()
            .test_name(test_name)
            .table_name(table_name)
            .sample_mode(sample_mode)
            .start_timing()
        )

    def build(self) -> TestResult:
        """Build the final test result object"""
        if self._test_name is None:
            raise ValueError("Test name is required")
        if self._table_name is None:
            raise ValueError("Table name is required")
        if self._passed is None:
            raise ValueError("Test result (passed/failed) is required")

        return TestResult(
            test_name=self._test_name,
            table_name=self._table_name,
            passed=self._passed,
            error_count=self._error_count,
            total_count=self._total_count,
            details=self._details,
            execution_time=self._execution_time,
            sample_mode=self._sample_mode,
            example_failures=self._example_failures,
        )

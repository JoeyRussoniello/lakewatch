"""Core test runner class that gets exposed and used in the CLI"""

from typing import Any, Dict, List

from pyspark.sql import SparkSession

from lakewatch.core.provider import DataProvider
from lakewatch.core.result import TestResult
from lakewatch.strategies.base import TestStrategy


class TestRunner:
    """Orchestrates test execution and result collection"""

    def __init__(self, data_provider: DataProvider):
        self.data_provider = data_provider
        self.test_results: List[TestResult] = []
        self.queued_tests: List[Dict[str, Any]] = []

    def add_test(
        self,
        test_strategy: TestStrategy,
        table_name: str,
        filter_column: str = "PROPERTY_UUID",
        disable_sampling: bool = False,
        **test_kwargs,
    ):
        """Add a test to the execution queue"""

        # If sample_mode is unspecified, use the data provider's default

        test_config = {
            "strategy": test_strategy,
            "table_name": table_name,
            "filter_column": filter_column,
            "disable_sampling": disable_sampling,
            "kwargs": test_kwargs,
        }
        self.queued_tests.append(test_config)
        print(f"Added test: {test_strategy.test_name} for {table_name} to queue")

    def clear_queue(self):
        """Clear the test queue"""
        self.queued_tests = []
        self.test_results = []
        print("Test queue cleared")

    def run_all_tests(self) -> List[TestResult]:
        """Execute all queued tests"""
        print("=" * 60)
        print("STARTING LAKEHOUSE DATA TESTING SUITE")
        print(f"Mode: {'SAMPLE' if self.data_provider.sample_mode else 'FULL DATASET'}")
        if self.data_provider.sample_mode:
            print(f"Sample Properties: {len(self.data_provider.sample_properties)}")
        print(f"Queued Tests: {len(self.queued_tests)}")
        print("=" * 60)

        self.test_results = []

        for test_config in self.queued_tests:
            try:
                strategy = test_config["strategy"]
                table_name = test_config["table_name"]
                filter_column = test_config["filter_column"]
                disable_sampling = test_config["disable_sampling"]
                test_kwargs = test_config["kwargs"]

                print(f"\n🔄 Running {strategy.test_name} on {table_name}...")

                df = self.data_provider.get_dataframe(
                    table_name, filter_column, disable_sampling
                )
                result = strategy.execute(
                    df,
                    table_name=table_name,
                    sample_mode=self.data_provider.sample_mode,
                    **test_kwargs,
                )
                self.test_results.append(result)

                status = "✅ PASSED" if result.passed else "❌ FAILED"
                print(f"{status} - {result.details}")

            except Exception as e:
                print(f"❌ Critical error running {strategy.test_name}: {str(e)}")
                error_result = TestResult(
                    strategy.test_name,
                    table_name,
                    False,
                    -1,
                    -1,
                    f"Critical error: {str(e)}",
                    0.0,
                    self.data_provider.sample_mode,
                )
                self.test_results.append(error_result)

        self._print_summary()
        return self.test_results

    def _print_summary(self):
        """Print test execution summary"""
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        if not self.test_results:
            print("No tests were run.")
            return

        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result.passed)
        failed_tests = total_tests - passed_tests

        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests / total_tests) * 100:.1f}%")
        print()

        for result in self.test_results:
            status = "✅ PASS" if result.passed else "❌ FAIL"
            print(
                f"{status:8} | {result.execution_time_seconds:6.2f}s | {result.test_name:40} | {result.details}"
            )

        if failed_tests > 0:
            print(f"\n⚠️  {failed_tests} test(s) failed. Review details above.")
        else:
            print("\n🎉 All tests passed!")


# TODO: Minor Refactor to avoid forcing sample mod configuration unless needed
def create_test_suite(
    spark_session: SparkSession,
    lakehouse_name: str,
    sample_dim_table: str,
    sample_dim_column: str,
) -> TestRunner:
    """Helper method to spin up a DataProvider (sampler) and Test Runner"""
    data_provider = DataProvider(
        spark_session, lakehouse_name, sample_dim_table, sample_dim_column
    )
    test_runner = TestRunner(data_provider)
    return test_runner

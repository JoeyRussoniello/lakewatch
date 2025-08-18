from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from lakewatch.core.result import TestResultBuilder
from lakewatch.strategies.base import TestStrategy, require_columns


class UniquenessTest(TestStrategy):
    """Test whether each combination of id_cols is unique"""

    def __init__(self, id_cols: list[str]):
        self.id_cols = id_cols

    @property
    def test_name(self) -> str:
        return f"Unique Columns, Subset: {self.id_cols}"

    def execute(self, df: DataFrame, table_name: str, **kwargs):
        sample_mode = kwargs.get("sample_mode", False)
        require_columns(df, self.id_cols, where=self.test_name)

        builder = TestResultBuilder.for_test(self.test_name, table_name, sample_mode)

        try:
            total_count = df.count()
            duplicates = df.groupBy(self.id_cols).count().filter(col("count") > 1)
            error_count = duplicates.count()

            details = f"Found {error_count} duplicates of {total_count} total rows"

            if error_count > 0:
                print(f"Samples with duplicate {self.id_cols}")
                duplicates.show(5)
                builder.cache_failure(duplicates)

            return (
                builder.auto_pass_fail(total_count, error_count, details)
                .end_timing()
                .build()
            )

        except Exception as e:
            return (
                builder.exception(e, f"Failed to test {self.id_cols} uniqueness")
                .end_timing()
                .build()
            )

# Lakewatch

Spark-first data quality tests for Lakehouse star schemas. Ship fast checks for effective-dating, per-entity daily continuity, and uniqueness. Runs comfortably in Microsoft Fabric notebooks today; designed to grow into a general framework you can extend with your own tests.

Status: early, evolving. APIs may change. Feedback and contributions are very welcome.

---

## What’s in this repo

The included notebook (`Test Suite.ipynb`) defines a minimal but working framework:

- `TestResult` and `TestResultBuilder` — consistent result objects with timing, counts, pass/fail, success rate, and (optional) tiny samples of failures for fast triage.
- `TestStrategy` — abstract base for all tests (plug-in point).
- `DataProvider` — reads from your Lakehouse and (optionally) filters to a sampled set of IDs to speed up runs.
- `TestRunner` — queues tests, executes them, prints a summary, and returns a list of TestResult.
- Built-in strategies (more coming):
    - `EffectiveDateValidityTest(id_col, start_col, end_col)` — asserts `start <= end`.
    - `UniquenessTest(id_cols: list[str])` — asserts uniqueness on a subset of columns (e.g., [entity_id, date]).
    - `DateContinuityTest(id_col: str | Column, date_col: str)` — asserts daily continuity (no gaps) within each `id_col`. Accepts a literal (e.g., `lit(1)`) to check continuity across the whole table (e.g., DIM_DATE).

Notes:
- In the notebook version, sampling pulls IDs from DIM_PROPERTY.Property_UUID. This behavior will be updated to reflect `src` patterns
- Exceptions are considered system failures (0% success) and are surfaced in the summary.
- Failure samples are small, driver-safe pandas frames serialized in `TestResult.to_dict()['failures']`.

---

## Use in Microsoft Fabric (right now)

Upload the provided `.ipynb` into your Fabric workspace, attach your Lakehouse, then in a Fabric Spark notebook:

```python
%run Test Suite.ipynb #Or whatever the testing suite is named
```

Example Session:

```python
# (Optional) enable sampling to go faster on large facts
test_prop_ids = test_runner.data_provider.enable_sample_mode(10)

# Define strategies
effective_date_valid = EffectiveDateValidityTest(
    id_col='UNIT_UUID',
    start_col='EFFECTIVE_START_DATE',
    end_col='EFFECTIVE_END_DATE'
)
ued_date_uniquess = UniquenessTest(id_cols=['UNIT_UUID', 'DATE'])
ued_date_continuity = DateContinuityTest(id_col='UNIT_UUID', date_col='DATE')
dim_date_continuity = DateContinuityTest(id_col=lit(1), date_col='DATE_KEY')  # no id dependency

# Queue tests
test_runner.add_test(effective_date_valid, 'FACT_UNIT_EVENT')
test_runner.add_test(ued_date_uniquess, 'FACT_UNIT_EVENT_DAILY')
test_runner.add_test(ued_date_continuity, 'FACT_UNIT_EVENT_DAILY')
test_runner.add_test(dim_date_continuity, 'DIM_DATE', disable_sampling=True)

# Run and view results
results = test_runner.run_all_tests()
results_df = spark.createDataFrame([r.to_dict() for r in results])
display(results_df.orderBy('passed', ascending=True))
```

## How it works (structures)

- `TestStrategy`
Implement `test_name` and `execute(df, table_name, sample_mode=...) -> TestResult`. Focus on pure Spark transforms + minimal actions.

- `TestRunner`
`add_test(strategy, table_name, filter_column='PROPERTY_UUID', disable_sampling=False, **kwargs)`
`run_all_tests() -> list[TestResult]`

- `DataProvider`
`enable_sample_mode(size, seed=None) -> list[str]` selects a deterministic set of IDs to filter facts (notebook version uses `DIM_PROPERTY.Property_UUID`).
`get_dataframe(table_name, filter_column='PROPERTY_UUID', disable_sampling=False) -> DataFrame` applies the sample filter when active.

- `TestResultBuilder`
Fluent builder used by strategies to keep timing, counts, pass/fail, and optional cache_failure(...) samples consistent.

## Extending: write your own test

Create a strategy by subclassing `TestStrategy`. You can reuse helpers like `require_columns(df, [cols], where=...)` and the result builder.

```python
class RowCountGreaterThanTest(TestStrategy):
    def __init__(self, min_rows: int):
        self.min_rows = min_rows

    @property
    def test_name(self) -> str:
        return f"Row count ≥ {self.min_rows}"

    def execute(self, df, table_name: str, **kwargs):
        sample_mode = bool(kwargs.get('sample_mode', False))
        builder = TestResultBuilder.for_test(self.test_name, table_name, sample_mode)

        try:
            total = df.count()
            errors = 0 if total >= self.min_rows else 1
            details = f"total={total}, required≥{self.min_rows}"
            return builder.auto_build(total, errors, details)
            # Alternatively,
            # return builder.auto_pass_fail(total, errors, details).end_timing().build()
            
        except Exception as e:
            return builder.exception(e, "Row count check failed").end_timing().build()
```

---

## Results and reporting

After `run_all_tests()` you get a list of `TestResult`:

```python
results = test_runner.run_all_tests()
rows = [r.to_dict() for r in results]
spark.createDataFrame(rows).show(truncate=False)
```

Each result includes: test_name, table_name, passed, error_count, total_count, success_rate, details, execution_time_seconds, timestamp, sample_mode, and optional failures (small sample of offending rows).

---

## Roadmap (brief)

- More built-in strategies (interval non-overlap, surrogate key completeness, referential integrity, nullability profiles).
- Config-driven runs and CLI.
- Reporters (JSON, JUnit, HTML).
- Pluggable providers and strategy registry.

If you try this in Fabric or adapt it for your lakehouse, please open an issue with your use case and any pain points—this is meant to be both useful out of the box and easy to extend with custom tests.

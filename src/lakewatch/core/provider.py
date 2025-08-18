"""provider: Includes DataProvider class for handling sampling
and improving performance on large datasets"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DataProvider:
    """
    Handles data access and optional sampling.

    Parameters
    ----------
    spark : SparkSession
    lakehouse_name : str
        Database/schema name (e.g., 'TRINITY_BI').
    sample_dim_table : str
        Table to draw sample IDs from (e.g., 'DIM_PROPERTY').
    sample_dim_column : str
        Column containing entity IDs (e.g., 'PROPERTY_UUID').
    """

    def __init__(
        self,
        spark: SparkSession,
        lakehouse_name: str,
        sample_dim_table: str,
        sample_dim_column: str,
    ):
        self.spark = spark
        self.lakehouse_name = lakehouse_name
        self.sample_mode = False
        self.sample_ids: list[Any] = []
        self.sample_table = sample_dim_table
        self.sample_id_col = sample_dim_column

    def set_sample_ids(self, ids: Sequence[Any]) -> list[Any]:
        """Helper to inject externally-determined IDs (potentially from prior run)"""
        self.sample_ids = list(ids)
        self.sample_mode = True
        print(
            f"Sample mode enabled with {len(self.sample_ids)} {self.sample_id_col}(s)"
        )
        return self.sample_ids

    def enable_sample_mode(
        self,
        sample_size: int = 10,
        random_seed: Optional[int] = None,
        distinct_ids: bool = True,
    ) -> list[Any]:
        """
        Enable sample mode by selecting IDs from the configured sample table.

        Notes
        - Uses `orderBy(rand()).limit(n)` for exact sample size; can be expensive on huge DIM tables
          You can swap to approximate `sample(fraction)` if you prefer speed over exactness.
        """
        print(f"Enabling sample mode with {sample_size} {self.sample_id_col}(s)...")

        id_df = self.spark.table(f"{self.lakehouse_name}.{self.sample_table}").select(
            self.sample_id_col
        )
        if distinct_ids:
            id_df = id_df.distinct()

        total_ids = id_df.count()

        if sample_size >= total_ids:
            self.sample_ids = [row[self.sample_id_col] for row in id_df.collect()]
        else:
            # Exact-size sample via randomized order + limit (simple & deterministic with seed)
            rnd = F.rand(random_seed) if random_seed is not None else F.rand()
            sampled_df = id_df.orderBy(rnd).limit(sample_size)
            self.sample_ids = [row[self.sample_id_col] for row in sampled_df.collect()]

        self.sample_mode = True
        print(
            f"Sample mode enabled with {len(self.sample_ids)} {self.sample_id_col}(s)"
        )
        return self.sample_ids

    def disable_sample_mode(self) -> None:
        """Disable sample mode and use full dataset for tests"""
        self.sample_mode = False
        self.sample_ids = []
        print("Sample mode disabled - tests will run on full dataset")

    def get_dataframe(
        self,
        table_name: str,
        filter_column: Optional[str] = None,
        disable_sampling: bool = False,
    ) -> DataFrame:
        """
        Return a DataFrame, optionally filtered to the current sample IDs.

        If `filter_column` is None and sampling is enabled, we default to `self.sample_id_col`.
        """
        df = self.spark.table(f"{self.lakehouse_name}.{table_name}")

        if disable_sampling or not (self.sample_mode and self.sample_ids):
            return df

        col_name = filter_column or self.sample_id_col
        if col_name not in df.columns:
            print(f"⚠️  Sampling disabled for {table_name}: no '{col_name}' column.")
            return df

        return df.filter(F.col(col_name).isin(self.sample_ids))

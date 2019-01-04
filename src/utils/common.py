from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_technical_columns(df: DataFrame, raw_file_path: str) -> DataFrame:
    """
    Add standard technical columns used across Bronze tables.
    - ingested_at: Timestamp when the row was processed
    - raw_file_path: Source file path
    """
    return (
        df.withColumn("ingested_at", F.current_timestamp())
          .withColumn("raw_file_path", F.lit(raw_file_path))
    )


def filter_out_aggregate_regions(df: DataFrame, country_col: str, regions: list[str]) -> DataFrame:
    """Remove aggregate regions to keep only country-level rows."""
    return df.where(~F.col(country_col).isin(regions))



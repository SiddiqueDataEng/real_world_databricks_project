from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_csv(
    spark: SparkSession,
    path: str,
    sep: str = ",",
    header: bool = True,
    quote: str = '"',
    escape: str = '"',
) -> DataFrame:
    return (
        spark.read.option("header", header)
        .option("sep", sep)
        .option("quote", quote)
        .option("escape", escape)
        .option("ignoreTrailingWhiteSpace", True)
        .option("ignoreLeadingWhiteSpace", True)
        .csv(path)
    )


def read_tsv(spark: SparkSession, path: str) -> DataFrame:
    return read_csv(spark, path, sep="\t")


def read_xml(
    spark: SparkSession,
    path: str,
    row_tag: str,
    infer_schema: bool = True,
) -> DataFrame:
    return (
        spark.read.format("xml")
        .option("rowTag", row_tag)
        .option("inferSchema", str(infer_schema).lower())
        .load(path)
    )


def read_fixed_width(
    spark: SparkSession,
    path: str,
    column_specs: Dict[str, Dict[str, int]],
    skip_rows: int = 0,
) -> DataFrame:
    """
    Load a fixed-width file into a DataFrame by first reading as text and
    then slicing substrings per column specs: {name: {start: int, length: int}}.
    'start' is 1-based like Excel; converted to Spark substring which is 1-based.
    """
    df_raw = spark.read.text(path).toDF("value")
    if skip_rows > 0:
        first_values = [r[0] for r in df_raw.limit(skip_rows).collect()]
        df_raw = df_raw.where(~F.col("value").isin(first_values))

    df = df_raw
    for col_name, spec in column_specs.items():
        start = int(spec["start"])  # 1-based
        length = int(spec["length"])  # number of chars
        df = df.withColumn(col_name, F.substring(F.col("value"), start, length))

    return df.drop("value")



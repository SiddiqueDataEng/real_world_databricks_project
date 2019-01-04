from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
)


def bronze_literacy_schema() -> StructType:
    return StructType(
        [
            StructField("country", StringType(), False),
            StructField("young_male_rate", IntegerType(), True),
            StructField("young_female_rate", IntegerType(), True),
            StructField("adult_male_rate", IntegerType(), True),
            StructField("adult_female_rate", IntegerType(), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("raw_file_path", StringType(), False),
        ]
    )


def bronze_children_schema() -> StructType:
    return StructType(
        [
            StructField("country", StringType(), False),
            StructField("children_out_school_male_2019", IntegerType(), True),
            StructField("children_out_school_female_2019", IntegerType(), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("raw_file_path", StringType(), False),
        ]
    )


def bronze_education_schema() -> StructType:
    return StructType(
        [
            StructField("country", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("value", DecimalType(10, 4), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("raw_file_path", StringType(), False),
        ]
    )


def bronze_population_schema() -> StructType:
    return StructType(
        [
            StructField("country", StringType(), False),
            StructField("population_millions", DecimalType(18, 6), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("raw_file_path", StringType(), False),
        ]
    )



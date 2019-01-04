import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


def create_spark(local_delta_dir: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("world_bank_demo_local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(local_delta_dir, "warehouse"))
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data" / "raw"
    lake_dir = base_dir / "_delta_lake"
    lake_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark(str(lake_dir))
    database_name = "world_bank_demo_local"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    spark.sql(f"USE {database_name}")

    # Paths (local file://)
    literacy_tsv = str((data_dir / "literacy.tsv").resolve())
    children_csv = str((data_dir / "children_out_of_school.csv").resolve())
    education_xml = str((data_dir / "education_expenditure.xml").resolve())
    population_fwf = str((data_dir / "world_population.fwf").resolve())

    # Bronze ingestion (mirrors notebook logic, simplified)
    lit_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("quote", '"').option("escape", '"')
        .csv(f"file://{literacy_tsv}")
    )
    for c in lit_df.columns:
        lit_df = lit_df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))
    lit_df = lit_df.withColumn("ingested_at", F.current_timestamp()).withColumn("raw_file_path", F.lit(literacy_tsv))
    lit_df.write.format("delta").mode("overwrite").saveAsTable("bronze_literacy")

    child_df = (
        spark.read.option("header", True)
        .option("sep", ",")
        .option("quote", '"').option("escape", '"')
        .csv(f"file://{children_csv}")
    )
    for c in child_df.columns:
        child_df = child_df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))
    child_df = child_df.withColumn("ingested_at", F.current_timestamp()).withColumn("raw_file_path", F.lit(children_csv))
    child_df.write.format("delta").mode("overwrite").saveAsTable("bronze_children")

    edu_df = (
        spark.read.format("xml").option("rowTag", "data").option("inferSchema", "true").load(f"file://{education_xml}")
    )
    edu_df = edu_df.withColumn("ingested_at", F.current_timestamp()).withColumn("raw_file_path", F.lit(education_xml))
    edu_df.write.format("delta").mode("overwrite").saveAsTable("bronze_education")

    raw = spark.read.text(f"file://{population_fwf}").toDF("value")
    headers = [r[0] for r in raw.limit(3).collect()]
    raw = raw.where(~F.col("value").isin(headers))
    pop_df = (
        raw.withColumn("country", F.trim(F.substring(F.col("value"), 1, 30)))
        .withColumn("population_millions", F.substring(F.col("value"), 31, 6).cast("decimal(18,6)"))
        .drop("value")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("raw_file_path", F.lit(population_fwf))
    )
    pop_df.write.format("delta").mode("overwrite").saveAsTable("bronze_population")

    # Silver
    lit = spark.table("bronze_literacy").select(
        F.col("country"),
        F.col("percent of male 15 to 24").cast("int").alias("young_male_rate"),
        F.col("percent of female 15 to 24").cast("int").alias("young_female_rate"),
        F.col("percent of male 15 and older").cast("int").alias("adult_male_rate"),
        F.col("percent of female 15 and older").cast("int").alias("adult_female_rate"),
        "ingested_at",
        "raw_file_path",
    )
    lit.write.format("delta").mode("overwrite").saveAsTable("silver_literacy")

    child = spark.table("bronze_children").select(
        F.col("country"),
        F.col("report: male children out of school in 2019").cast("int").alias("children_out_school_male_2019"),
        F.col("report: female children out of school in 2019").cast("int").alias("children_out_school_female_2019"),
        "ingested_at",
        "raw_file_path",
    )
    child.write.format("delta").mode("overwrite").saveAsTable("silver_children")

    edu = spark.table("bronze_education")
    if "field" in edu.columns and "name" not in edu.columns:
        edu = edu.select(F.posexplode("field").alias("pos", "elem"), "*")
        edu = edu.withColumn("name", F.col("elem._name")).withColumn("val", F.col("elem._value")).drop("elem")
        edu = edu.groupBy("field").pivot("name").agg(F.first("val"))
        edu = edu.select(
            F.col("Country or Area").alias("country"),
            F.col("Year").cast("int").alias("year"),
            F.col("Value").cast("decimal(10,4)").alias("value"),
        )
    else:
        cn = [c for c in edu.columns]
        c_country = next((c for c in cn if c.lower() in ["country", "country_or_area", "country or area"]), "country")
        c_year = next((c for c in cn if c.lower() == "year"), "year")
        c_value = next((c for c in cn if c.lower() in ["value", "val"]), "value")
        edu = edu.select(F.col(c_country).alias("country"), F.col(c_year).cast("int").alias("year"), F.col(c_value).cast("decimal(10,4)").alias("value"))
    edu.write.format("delta").mode("overwrite").saveAsTable("silver_education")

    pop = spark.table("bronze_population").select(
        F.trim(F.col("country")).alias("country"),
        F.col("population_millions").cast("decimal(18,6)").alias("population_millions"),
        "ingested_at",
        "raw_file_path",
    )
    pop.write.format("delta").mode("overwrite").saveAsTable("silver_population")

    # Gold KPIs (2019)
    lit = spark.table("silver_literacy")
    child = spark.table("silver_children")
    edu = spark.table("silver_education").where(F.col("year") == 2019)
    pop = spark.table("silver_population")

    child2019 = child.select(
        "country",
        (F.col("children_out_school_male_2019") + F.col("children_out_school_female_2019")).alias("children_out_school_total_2019"),
    )

    df = (
        lit.alias("l")
        .join(child2019.alias("c"), on="country", how="inner")
        .join(edu.alias("e"), on="country", how="inner")
        .join(pop.alias("p"), on="country", how="inner")
    )
    df = df.withColumn("gov_education_exp", F.col("e.value"))
    df.write.format("delta").mode("overwrite").saveAsTable("gold_kpis_2019")

    q1 = (
        df.select(
            "country",
            (F.col("children_out_school_total_2019") / (F.col("population_millions") * F.lit(1_000_000))).alias("pct_children_out_school"),
        )
        .orderBy(F.col("pct_children_out_school").desc())
        .limit(1)
    )
    q2 = (
        df.select(
            "country",
            (F.col("young_male_rate") + F.col("young_female_rate")).alias("young_total"),
            (F.col("adult_male_rate") + F.col("adult_female_rate")).alias("adult_total"),
        )
        .select("country", (F.col("young_total") - F.col("adult_total")).alias("gap_literacy"))
        .orderBy(F.col("gap_literacy").desc())
        .limit(1)
    )
    top10 = df.select("country", F.col("gov_education_exp")).orderBy(F.col("gov_education_exp").desc()).limit(10)
    avg_adult = (
        df.join(top10, on="country")
        .select(((F.col("adult_male_rate") + F.col("adult_female_rate")) / F.lit(2)).alias("adult_literacy_rate"))
        .agg(F.avg("adult_literacy_rate").alias("avg_adult_literacy_top10_spend"))
    )

    print("Q1:")
    q1.show(truncate=False)
    print("Q2:")
    q2.show(truncate=False)
    print("Q3:")
    avg_adult.show(truncate=False)


if __name__ == "__main__":
    main()



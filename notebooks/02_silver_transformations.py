# Databricks notebook source
dbutils.widgets.text("database_name", "world_bank_demo")
database_name = dbutils.widgets.get("database_name")
spark.sql(f"USE {database_name}")

from pyspark.sql import functions as F

bronze_literacy = "bronze_literacy"
bronze_children = "bronze_children"
bronze_education = "bronze_education"
bronze_population = "bronze_population"

silver_literacy = "silver_literacy"
silver_children = "silver_children"
silver_education = "silver_education"
silver_population = "silver_population"


# Literacy: cast numeric columns, standardize names
lit = spark.table(bronze_literacy)
lit = (
    lit.select(
        F.col("country").alias("country"),
        F.col("young_male_rate").cast("int").alias("young_male_rate"),
        F.col("young_female_rate").cast("int").alias("young_female_rate"),
        F.col("adult_male_rate").cast("int").alias("adult_male_rate"),
        F.col("adult_female_rate").cast("int").alias("adult_female_rate"),
        "ingested_at",
        "raw_file_path",
    )
)
lit.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(silver_literacy)


# Children: cast numeric columns
child = spark.table(bronze_children)
child = (
    child.select(
        F.col("country").alias("country"),
        F.col("children_out_school_male_2019").cast("int").alias("children_out_school_male_2019"),
        F.col("children_out_school_female_2019").cast("int").alias("children_out_school_female_2019"),
        "ingested_at",
        "raw_file_path",
    )
)
child.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(silver_children)


# Education expenditure: explode XML fields if needed, keep only country, year, value
edu = spark.table(bronze_education)
if "field" in edu.columns and "name" not in edu.columns:
    # XML exploded structure as in SRT steps: field is array<struct<name,key,value>>
    edu = edu.select(F.posexplode("field").alias("pos", "elem"), "*")
    edu = edu.withColumn("name", F.col("elem._name")).withColumn("val", F.col("elem._value")).drop("elem")
    # Pivot into columns
    edu = (
        edu.groupBy("field")
        .pivot("name")
        .agg(F.first("val"))
    )
    # Expect columns: Country or Area, Year, Value
    edu = edu.select(
        F.col("Country or Area").alias("country"),
        F.col("Year").cast("int").alias("year"),
        F.col("Value").cast("decimal(10,4)").alias("value"),
    )
else:
    # Already flattened: try common names
    cn = [c for c in edu.columns]
    c_country = next((c for c in cn if c.lower() in ["country", "country_or_area", "country or area"]), "country")
    c_year = next((c for c in cn if c.lower() == "year"), "year")
    c_value = next((c for c in cn if c.lower() in ["value", "val"]), "value")
    edu = edu.select(
        F.col(c_country).alias("country"),
        F.col(c_year).cast("int").alias("year"),
        F.col(c_value).cast("decimal(10,4)").alias("value"),
    )

edu.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(silver_education)


# Population: trim country, cast population
pop = spark.table(bronze_population)
pop = (
    pop.select(
        F.trim(F.col("country")).alias("country"),
        F.col("population_millions").cast("decimal(18,6)").alias("population_millions"),
        "ingested_at",
        "raw_file_path",
    )
)
pop.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(silver_population)

# Print counts
for t in [silver_literacy, silver_children, silver_education, silver_population]:
    print(t, spark.table(t).count())



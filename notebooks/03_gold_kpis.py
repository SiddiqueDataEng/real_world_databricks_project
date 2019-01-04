# Databricks notebook source
dbutils.widgets.text("database_name", "world_bank_demo")
database_name = dbutils.widgets.get("database_name")
spark.sql(f"USE {database_name}")

from pyspark.sql import functions as F

silver_literacy = "silver_literacy"
silver_children = "silver_children"
silver_education = "silver_education"
silver_population = "silver_population"

gold_kpis_2019 = "gold_kpis_2019"

lit = spark.table(silver_literacy)
child = spark.table(silver_children)
edu = spark.table(silver_education).where(F.col("year") == 2019)
pop = spark.table(silver_population)

# Aggregate children male+female 2019 -> total
child2019 = (
    child.select(
        "country",
        (F.col("children_out_school_male_2019") + F.col("children_out_school_female_2019")).alias("children_out_school_total_2019"),
    )
)

df = (
    lit.alias("l")
    .join(child2019.alias("c"), on="country", how="inner")
    .join(edu.alias("e"), on="country", how="inner")
    .join(pop.alias("p"), on="country", how="inner")
)

# Normalize education spend column for easier reference
df = df.withColumn("gov_education_exp", F.col("e.value"))

# Persist combined KPI view for 2019
(
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(gold_kpis_2019)
)

# Q1: Country with most children out of school as % of total population
q1 = (
    df.select(
        "country",
        (F.col("children_out_school_total_2019") / (F.col("population_millions") * F.lit(1_000_000))).alias("pct_children_out_school"),
    )
    .orderBy(F.col("pct_children_out_school").desc())
    .limit(1)
)
display(q1)

# Q2: Country with largest gap between young and adult literacy
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
display(q2)

# Q3: Average adult literacy rate for top 10 countries by education spend
top10 = (
    df.select("country", "gov_education_exp")
    .orderBy(F.col("gov_education_exp").desc())
    .limit(10)
)
avg_adult_lit = (
    df.join(top10, on="country", how="inner")
    .select(
        ((F.col("adult_male_rate") + F.col("adult_female_rate")) / F.lit(2)).alias("adult_literacy_rate")
    )
    .agg(F.avg("adult_literacy_rate").alias("avg_adult_literacy_top10_spend"))
)
display(avg_adult_lit)



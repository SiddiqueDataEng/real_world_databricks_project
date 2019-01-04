# Databricks notebook source
# Widgets for configuration (override as needed)
dbutils.widgets.text("database_name", "world_bank_demo")
dbutils.widgets.text("raw_base_path", "dbfs:/FileStore/world_bank_demo")
dbutils.widgets.text("literacy_tsv", "literacy.tsv")
dbutils.widgets.text("children_csv", "children_out_of_school.csv")
dbutils.widgets.text("education_xml", "education_expenditure.xml")
dbutils.widgets.text("xml_row_tag", "data")
dbutils.widgets.text("population_fwf", "world_population.fwf")
dbutils.widgets.text("fwf_skip_rows", "3")

database_name = dbutils.widgets.get("database_name")
raw_base_path = dbutils.widgets.get("raw_base_path")
literacy_tsv = f"{raw_base_path}/{dbutils.widgets.get('literacy_tsv')}"
children_csv = f"{raw_base_path}/{dbutils.widgets.get('children_csv')}"
education_xml = f"{raw_base_path}/{dbutils.widgets.get('education_xml')}"
xml_row_tag = dbutils.widgets.get("xml_row_tag")
population_fwf = f"{raw_base_path}/{dbutils.widgets.get('population_fwf')}"
fwf_skip_rows = int(dbutils.widgets.get("fwf_skip_rows"))

# Table names
bronze_literacy = "bronze_literacy"
bronze_children = "bronze_children"
bronze_education = "bronze_education"
bronze_population = "bronze_population"

# Create and use database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

from pyspark.sql import functions as F


def add_technical_cols(df, raw_path):
    return df.withColumn("ingested_at", F.current_timestamp()).withColumn("raw_file_path", F.lit(raw_path))


def filter_regions(df, country_col="country"):
    regions = [
        "Europe & Central Asia (excluding high income)",
        "Central Europe and the Baltics",
        "East Asia & Pacific (excluding high income)",
        "World",
    ]
    return df.where(~F.col(country_col).isin(regions))


# 1) Youth & Adult Literacy (TSV)
lit_df = (
    spark.read.option("header", True)
    .option("sep", "\t")
    .option("quote", '"').option("escape", '"')
    .option("ignoreTrailingWhiteSpace", True)
    .option("ignoreLeadingWhiteSpace", True)
    .csv(literacy_tsv)
)

# Replace empty strings with nulls
for c in lit_df.columns:
    lit_df = lit_df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

# Heuristic rename to standard names if present
rename_map = {
    "country": "country",
    "percent of male 15 to 24": "young_male_rate",
    "percent of female 15 to 24": "young_female_rate",
    "percent of male 15 and older": "adult_male_rate",
    "percent of female 15 and older": "adult_female_rate",
}

def normalize_col(col_name: str) -> str:
    return col_name.strip().lower().replace("%", "percent").replace("-", " ")

cols = lit_df.columns
for c in cols:
    norm = normalize_col(c)
    for key, target in rename_map.items():
        if key in norm and c != target:
            lit_df = lit_df.withColumnRenamed(c, target)

lit_df = add_technical_cols(lit_df, literacy_tsv)
if "country" in lit_df.columns:
    lit_df = filter_regions(lit_df, "country")

(
    lit_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(bronze_literacy)
)


# 2) Children Out of School (CSV)
child_df = (
    spark.read.option("header", True)
    .option("sep", ",")
    .option("quote", '"').option("escape", '"')
    .option("ignoreTrailingWhiteSpace", True)
    .option("ignoreLeadingWhiteSpace", True)
    .csv(children_csv)
)
for c in child_df.columns:
    child_df = child_df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

child_rename = {
    "country": "country",
    "report: male children out of school in 2019": "children_out_school_male_2019",
    "report: female children out of school in 2019": "children_out_school_female_2019",
}

for c in child_df.columns:
    norm = normalize_col(c)
    for key, target in child_rename.items():
        if key in norm and c != target:
            child_df = child_df.withColumnRenamed(c, target)

child_df = add_technical_cols(child_df, children_csv)
if "country" in child_df.columns:
    child_df = filter_regions(child_df, "country")

(
    child_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(bronze_children)
)


# 3) Government Education Expenditure (XML)
# Requires library: com.databricks:spark-xml_2.12:0.16.0
edu_df = (
    spark.read.format("xml").option("rowTag", xml_row_tag).option("inferSchema", "true").load(education_xml)
)
edu_df = add_technical_cols(edu_df, education_xml)

(
    edu_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(bronze_education)
)


# 4) World Population (fixed-width)
raw = spark.read.text(population_fwf).toDF("value")
if fwf_skip_rows > 0:
    header_vals = [r[0] for r in raw.limit(fwf_skip_rows).collect()]
    raw = raw.where(~F.col("value").isin(header_vals))

pop_df = (
    raw.withColumn("country", F.trim(F.substring(F.col("value"), 1, 30)))
        .withColumn("population_millions", F.substring(F.col("value"), 31, 6))
        .drop("value")
)

pop_df = pop_df.withColumn("population_millions", F.col("population_millions").cast("decimal(18,6)"))
pop_df = add_technical_cols(pop_df, population_fwf)
pop_df = filter_regions(pop_df, "country")

(
    pop_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(bronze_population)
)

# Display quick counts
for t in [bronze_literacy, bronze_children, bronze_education, bronze_population]:
    print(t, spark.table(t).count())



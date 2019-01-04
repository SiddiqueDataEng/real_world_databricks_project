import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession


def main():
    base_dir = Path(__file__).resolve().parents[1]
    lake_dir = base_dir / "_delta_lake"
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("world_bank_demo_visualize")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(str(lake_dir), "warehouse"))
        .getOrCreate()
    )
    spark.sql("USE world_bank_demo_local")

    df = spark.table("gold_kpis_2019")
    # Use derived columns for charting
    pdf = (
        df.select(
            "country",
            ((df["children_out_school_total_2019"] / (df["population_millions"] * 1_000_000))).alias("pct_children_out_school"),
            ((df["young_male_rate"] + df["young_female_rate"]) / 2.0).alias("young_literacy_avg"),
            ((df["adult_male_rate"] + df["adult_female_rate"]) / 2.0).alias("adult_literacy_avg"),
            df["gov_education_exp"].alias("gov_education_exp"),
        )
        .toPandas()
    )

    # Bar: Top 10 countries by gov education spend
    top10 = pdf.sort_values("gov_education_exp", ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    plt.barh(top10["country"][::-1], top10["gov_education_exp"][::-1])
    plt.title("Top 10 Government Education Expenditure (2019)")
    plt.xlabel("% of total expenditure")
    plt.tight_layout()
    plt.savefig(output_dir / "top10_education_spend.png")
    plt.close()

    # Scatter: Education spend vs Adult literacy
    plt.figure(figsize=(8, 6))
    plt.scatter(pdf["gov_education_exp"], pdf["adult_literacy_avg"], alpha=0.7)
    plt.title("Education Spend vs Adult Literacy (2019)")
    plt.xlabel("Gov education expend. (% total)")
    plt.ylabel("Adult literacy avg")
    plt.tight_layout()
    plt.savefig(output_dir / "spend_vs_adult_literacy.png")
    plt.close()

    # Bar: Top 10 children out of school (% of pop)
    top10_children = pdf.sort_values("pct_children_out_school", ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    plt.barh(top10_children["country"][::-1], (100 * top10_children["pct_children_out_school"][::-1]))
    plt.title("Top 10 Children Out of School (% of population, 2019)")
    plt.xlabel("% of population")
    plt.tight_layout()
    plt.savefig(output_dir / "top10_children_out_of_school.png")
    plt.close()

    print(f"Saved charts to: {output_dir}")


if __name__ == "__main__":
    main()



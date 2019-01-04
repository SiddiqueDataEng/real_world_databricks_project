import csv
import os
import random
import textwrap
from datetime import datetime

random.seed(42)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_literacy_tsv(path: str, countries: list[str], aggregates: list[str]) -> str:
    headers = [
        "country",
        "percent of male 15 to 24",
        "percent of female 15 to 24",
        "percent of male 15 and older",
        "percent of female 15 and older",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write("\t".join(headers) + "\n")
        for c in countries + aggregates:
            ym = random.randint(80, 100)
            yf = random.randint(80, 100)
            am = max(0, ym - random.randint(0, 10))
            af = max(0, yf - random.randint(0, 10))
            # sprinkle some blanks
            if random.random() < 0.1:
                af_str = ""
            else:
                af_str = str(af)
            row = [c, str(ym), str(yf), str(am), af_str]
            f.write("\t".join(row) + "\n")
    return path


def write_children_csv(path: str, countries: list[str], aggregates: list[str]) -> str:
    headers = [
        "country",
        "report: male children out of school in 2019",
        "report: female children out of school in 2019",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for c in countries + aggregates:
            male = random.randint(10_000, 500_000)
            female = random.randint(10_000, 500_000)
            if random.random() < 0.1:
                male_str = ""
            else:
                male_str = str(male)
            writer.writerow([c, male_str, str(female)])
    return path


def write_education_xml(path: str, countries: list[str]) -> str:
    # Generate spark-xml compatible structure: <data><field _name=... _value=... /></data>
    # We'll create a root with multiple <data> entries
    years = list(range(2018, 2021))  # keep small, 2018-2020
    lines = ["<root>"]
    for country in countries:
        for y in years:
            value = round(random.uniform(3.0, 8.0), 2)
            lines.append("  <data>")
            lines.append(
                "    <field _name=\"Country or Area\" _value=\"{}\" />".format(country)
            )
            lines.append(
                "    <field _name=\"Item\" _value=\"Government expenditure on education (% of total exp.)\" />"
            )
            lines.append("    <field _name=\"Year\" _value=\"{}\" />".format(y))
            lines.append("    <field _name=\"Value\" _value=\"{}\" />".format(value))
            lines.append("  </data>")
    lines.append("</root>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def write_population_fwf(path: str, countries: list[str], aggregates: list[str]) -> str:
    # 3 header rows to be skipped
    header = textwrap.dedent(
        f"""
        Generated: {datetime.utcnow().isoformat()}Z
        World Population (in millions)
        Country                        PopM
        """
    ).strip("\n")
    lines = header.splitlines()

    def fmt(country: str, pop_millions: int) -> str:
        # country: 30 chars, population: 6 chars
        c = country[:30].ljust(30)
        p = f"{pop_millions:06d}"
        return c + p

    for c in countries + aggregates:
        pop = random.randint(1, 300)  # in millions
        lines.append(fmt(c, pop))

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def main(output_dir: str = "real_world_databricks_project/data/raw") -> None:
    ensure_dir(output_dir)

    countries = ["Italy", "France", "India", "Brazil", "Japan"]
    aggregates = [
        "Central Europe and the Baltics",
        "Europe & Central Asia (excluding high income)",
    ]

    files = {}
    files["literacy_tsv"] = write_literacy_tsv(os.path.join(output_dir, "literacy.tsv"), countries, aggregates)
    files["children_csv"] = write_children_csv(os.path.join(output_dir, "children_out_of_school.csv"), countries, aggregates)
    files["education_xml"] = write_education_xml(os.path.join(output_dir, "education_expenditure.xml"), countries)
    files["population_fwf"] = write_population_fwf(os.path.join(output_dir, "world_population.fwf"), countries, aggregates)

    print("Generated files:")
    for k, v in files.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()



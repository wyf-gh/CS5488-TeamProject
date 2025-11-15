from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, quarter, avg, format_number, col, when, concat, lit
import os
import sys
import logging

# Suppress verbose Spark logs
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# === 1. Initialize Spark session ===
spark = SparkSession.builder \
    .appName("MacroEconomicQuarterly") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

# === 2. Define input and output paths ===
input_path = "cleaned_data/macro_economic_new.csv"
output_path = "macro_economic_quarterly.csv"

if not os.path.exists(input_path):
    print(f"[ERROR] Input file not found: {input_path}")
    spark.stop()
    sys.exit(1)

# === 3. Read CSV ===
df = spark.read.csv(input_path, header=False, inferSchema=True)

# Rename columns
df = df.withColumnRenamed("_c0", "Region") \
       .withColumnRenamed("_c1", "Original_Category")  \
       .withColumnRenamed("_c2", "Original_Indicator")  \
       .withColumnRenamed("_c4", "Date") \
       .withColumnRenamed("_c5", "Value")

df = df.withColumn(
    "Indicator",  # Create the new "Category" column here (fixes the "unresolved column" error)
    when(
        col("Original_Indicator").isin("Exports", "Imports"),  # Check RAW Category value
        concat(lit("Foreign Trade "), col("Original_Indicator"))  # Prepend "Foreign Trade "
    ).otherwise(
        col("Original_Indicator")  # Use RAW Indicator for other categories
    )
)

df = df.drop("Original_Category", "Original_Indicator", "Frequency")

# === 4. Convert Date column ===
df = df.withColumn("Date", to_date(df["Date"], "yyyy/M/d"))

# === 5. Add Year and Quarter columns ===
df = df.withColumn("Year", year(df["Date"])) \
       .withColumn("Quarter", quarter(df["Date"]))

# === 6. Group by and calculate quarterly averages ===
quarterly_df = df.groupBy("Region", "Indicator", "Year", "Quarter") \
    .agg(avg("Value").alias("Value"))

# === 7. Keep two decimal places ===
quarterly_df = quarterly_df.select(
    "Region",
    "Indicator",
    "Year",
    "Quarter",
    format_number("Value", 4).alias("Value")
)

# === 8. Write to CSV with header ===
temp_output = "macro_quarterly_temp"
quarterly_df.orderBy("Region", "Indicator", "Year", "Quarter") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(temp_output)

# === 9. Move and rename output file ===
import glob, shutil

part_file = glob.glob(os.path.join(temp_output, "part-*.csv"))
if part_file:
    shutil.move(part_file[0], output_path)
    shutil.rmtree(temp_output)

# === 10. Validate output row count ===
output_df = spark.read.csv(output_path, header=True, inferSchema=True)
row_count = output_df.count()
print(f"[INFO] Quarterly data saved to: {output_path}")
print(f"[INFO] Total rows in output: {row_count}")

# === 11. Stop Spark safely ===
spark.stop()
print("[INFO] Spark stopped successfully.")

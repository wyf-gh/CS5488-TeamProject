from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, quarter, avg, round, coalesce,
    length, when, concat, lit
)
import logging, shutil, os, glob

# 1. Configure Spark logging
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# 2. Initialize Spark session
spark = SparkSession.builder \
    .appName("CentralBankQuarterly_Aggregation") \
    .config("spark.local.dir", "D:/spark-temp") \
    .getOrCreate()

# 3. Read the input CSV file (update the path as needed)
input_path = "C:/Users/wyf/projects/big_data/cleaned_data/central_bank_new.csv"
raw = spark.read.csv(input_path, header=False, inferSchema=True)

# 4. Rename columns according to the provided layout:
# Country (region), Category (data type), Indicator, Frequency, Unit, Date, Value
df = raw.withColumnRenamed("_c0", "Country") \
        .withColumnRenamed("_c1", "Category") \
        .withColumnRenamed("_c2", "Indicator") \
        .withColumnRenamed("_c3", "Frequency") \
        .withColumnRenamed("_c4", "Unit") \
        .withColumnRenamed("_c5", "DateRaw") \
        .withColumnRenamed("_c6", "ValueRaw")

# 5. Prepare Date string: convert to string, handle monthly-like values "YYYY/MM" by appending "/01"
# and attempt multiple parse patterns (slashes and hyphens, single or double digit months/days).
date_str = col("DateRaw").cast("string")
date_str_fixed = when(length(date_str) <= 7, concat(date_str, lit("/01"))).otherwise(date_str)

# 6. Parse date using several common patterns; coalesce will pick the first successful parse
parsed_date = coalesce(
    to_date(date_str_fixed, "yyyy/M/d"),
    to_date(date_str_fixed, "yyyy-M-d"),
    to_date(date_str_fixed, "yyyy/MM/dd"),
    to_date(date_str_fixed, "yyyy-MM-dd")
)
df = df.withColumn("Date", parsed_date)

# 7. Cast value to double (safe): if value is null or non-numeric it becomes null
df = df.withColumn("Value", col("ValueRaw").cast("double"))

# 8. Add Year and Quarter columns
df = df.withColumn("Year", year(col("Date"))) \
       .withColumn("Quarter", quarter(col("Date")))


df_filtered = df.filter(
    (col("Country") != "US")  # 非美国数据不过滤
    | (col("Category") != "Interest Rate")  # 美国非利率数据不过滤（修正为大写R）
    | (col("Indicator") == "Fed Funds Target Rate")  # 美国利率仅留目标指标
)



# 9. Aggregate to quarterly averages (使用过滤后的数据 df_filtered)
# Grouping keys: Country (region), Category (data type), Indicator, Year, Quarter
quarterly = df_filtered.groupBy("Country", "Category", "Indicator", "Year", "Quarter") \
    .agg(round(avg("Value"), 4).alias("Value"))

# 10. Reorder and rename columns to exactly match requested output:
# Region, Indicator, Year, Quarter, Value
output_df = quarterly.select(
    col("Country").alias("Region"),
    col("Category").alias("Indicator"),
    col("Year"),
    col("Quarter"),
    col("Value")
).orderBy("Region", "Indicator", "Year", "Quarter")

# 11. Write to a single CSV with header
output_path = "C:/Users/wyf/projects/big_data/central_bank_quarterly"
temp_path = output_path + "_temp"

output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)

# 12. Move the single part file out and remove temp folder
part_files = glob.glob(os.path.join(temp_path, "part-*.csv"))
if not part_files:
    print("Error: no part file found in temporary output directory:", temp_path)
else:
    part_file = part_files[0]
    final_file = output_path + ".csv"
    # If final file exists, remove it first
    if os.path.exists(final_file):
        os.remove(final_file)
    shutil.move(part_file, final_file)
    shutil.rmtree(temp_path)

    # 13. Verify output file existence and line count
    if os.path.exists(final_file):
        with open(final_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            num_lines = len(lines)
        print("File successfully generated:", final_file)
        print("Total lines (including header):", num_lines)
    else:
        print("Output file not found after moving part file. Check permissions.")

# 14. Stop Spark cleanly
spark.stop()
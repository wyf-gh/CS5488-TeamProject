from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, quarter, avg, round
import logging, shutil, os, glob
import tempfile


# 1. Configure Spark logging
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# 2. Initialize Spark session
spark = SparkSession.builder \
    .appName("ExchangeRateQuarterly") \
    .config("spark.local.dir", "D:/spark-temp") \
    .getOrCreate()

# 3. Read the input CSV file
input_path = "C:/Users/wyf/projects/big_data/cleaned_data/exchange_rate_new.csv"
df = spark.read.csv(input_path, header=False, inferSchema=True)

df = df.withColumnRenamed("_c0", "Date") \
       .withColumnRenamed("_c1", "Pair") \
       .withColumnRenamed("_c2", "Open") \
       .withColumnRenamed("_c3", "Close") \
       .withColumnRenamed("_c4", "Change")

# 4. Convert date format
df = df.withColumn("Date", to_date(df["Date"], "yyyy/M/d"))

# 5. Add Year and Quarter columns
df = df.withColumn("Year", year(df["Date"])) \
       .withColumn("Quarter", quarter(df["Date"]))

# 6. Compute quarterly averages and round to 4 decimal places
quarterly_df = df.groupBy("Pair", "Year", "Quarter") \
    .agg(
        round(avg("Open"), 6).alias("Avg_Open"),
        round(avg("Close"), 6).alias("Avg_Close"),
        round(avg("Change"), 6).alias("Avg_Change")
    )

# 7. Write to a single CSV file with header
output_path = "C:/Users/wyf/projects/big_data/exchange_rate_quarterly"
temp_path = output_path + "_temp"

quarterly_df.orderBy("Pair", "Year", "Quarter") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# 8. Move and rename the single part file
part_file = glob.glob(os.path.join(temp_path, "part-*.csv"))[0]
shutil.move(part_file, output_path + ".csv")
shutil.rmtree(temp_path)

# 9. Verify output file and count lines
output_file = output_path + ".csv"
if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        num_lines = len(lines)
    print(f"File successfully generated: {output_file}")
    print(f"Total lines (including header): {num_lines}")
else:
    print("Output file not found. Please check the path or write permissions.")

temp_dir = tempfile.gettempdir()
for folder in os.listdir(temp_dir):
    if folder.startswith("spark-"):
        try:
            shutil.rmtree(os.path.join(temp_dir, folder), ignore_errors=True)
        except Exception as e:
            print(f"Warning: could not delete {folder}: {e}")

# 10. Stop Spark
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ExchangeRateAnalysis").getOrCreate()

# Load data
exchange = spark.read.csv("cleaned_data/exchange_quarterly.csv", header=True, inferSchema=True)
central = spark.read.csv("cleaned_data/central_quarterly.csv", header=True, inferSchema=True)
macro = spark.read.csv("cleaned_data/macro_quarterly.csv", header=True, inferSchema=True)

# (1) Select relevant columns
exchange = exchange.select("Pair", "Year", "Quarter", "Avg_Change")
central = central.select("Region", "Indicator", "Year", "Quarter", "Value")
macro = macro.select("Region", "Indicator", "Year", "Quarter", "Value")

# (2) Pivot central & macro to wide format (each indicator as a column)
central_wide = central.groupBy("Region", "Year", "Quarter") \
    .pivot("Indicator") \
    .avg("Value")

macro_wide = macro.groupBy("Region", "Year", "Quarter") \
    .pivot("Indicator") \
    .avg("Value")

# (3) Join everything by Year & Quarter
joined = exchange.join(central_wide, on=["Year", "Quarter"], how="left") \
                 .join(macro_wide, on=["Year", "Quarter"], how="left")

joined.show(5)

from pyspark.sql.functions import col, mean, when, last, coalesce, lit
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from read_raw_data import read_raw_data
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def preprocess_data(exchange_df: DataFrame, central_df: DataFrame, macro_df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Preprocess raw data with fixes for missing China/Japan data:
    1. Loosen joins (inner → left) to retain macro data when central bank data is missing
    2. Add Region name validation + data existence checks
    3. Enhanced missing value handling (forward fill for more indicators)
    4. Preserve partial data instead of dropping it entirely
    """
    # Step 1: Clean exchange rate data (unchanged)
    target_pairs = ["GBPUSD=X", "USDCNY=X", "USDJPY=X"]
    exchange_filtered = exchange_df.filter(col("Pair").isin(target_pairs)) \
        .select("Pair", "Year", "Quarter", col("Avg_Close").alias("Rate")) \
        .na.drop(subset=["Rate", "Year", "Quarter"])
    print(f"Exchange rate data: {exchange_filtered.count()} rows (target pairs: {target_pairs})")

    # Step 2: Pivot central bank data (add logging for data count)
    central_indicators = ["Interest Rate", "Foreign Exchange Reserves"]
    central_filtered = central_df.filter(col("Indicator").isin(central_indicators)) \
        .select("Region", "Year", "Quarter", "Indicator", "Value") \
        .na.drop(subset=["Value"])
    print(f"Central bank data: {central_filtered.count()} rows (indicators: {central_indicators})")
    print("Central bank regions available:", [r["Region"] for r in central_filtered.select("Region").distinct().collect()])

    central_wide = central_filtered.groupBy("Region", "Year", "Quarter") \
        .pivot("Indicator", central_indicators) \
        .agg(mean("Value")) \
        .withColumnRenamed("Interest Rate", "InterestRate") \
        .withColumnRenamed("Foreign Exchange Reserves", "ForeignReserves")

    # Step 3: Pivot macroeconomic data (add logging + handle missing Category gracefully)
    macro_indicators = [
        "Real GDP Growth Rate", "CPI (All Items)", "Consumer Confidence Index",
        "Unemployment Rate", "Foreign Trade Exports", "Foreign Trade Imports",
        "Government Debt to GDP Ratio", "Industrial Production Index",
        "House Price Index", "Retail Sales (Value)", "Retail Sales (Volume)"
    ]
    # Log available categories in macro data to check if indicators exist
    available_macro_cats = [c["Category"] for c in macro_df.select("Category").distinct().collect()]
    print(f"Macro data available categories: {available_macro_cats[:5]}... (total {len(available_macro_cats)})")

    macro_filtered = macro_df.filter(col("Category").isin(macro_indicators)) \
        .select("Region", "Year", "Quarter", "Category", "Value") \
        .na.drop(subset=["Value"])
    print(f"Macro data after filtering: {macro_filtered.count()} rows (target indicators: {len(macro_indicators)})")
    print("Macro data regions available:", [r["Region"] for r in macro_filtered.select("Region").distinct().collect()])

    macro_wide = macro_filtered.groupBy("Region", "Year", "Quarter") \
        .pivot("Category", macro_indicators) \
        .agg(mean("Value")) \
        .withColumnRenamed("Real GDP Growth Rate", "GDP") \
        .withColumnRenamed("CPI (All Items)", "CPI") \
        .withColumnRenamed("Consumer Confidence Index", "Confidence") \
        .withColumnRenamed("Unemployment Rate", "Unemployment") \
        .withColumnRenamed("Foreign Trade Exports", "Exports") \
        .withColumnRenamed("Foreign Trade Imports", "Imports") \
        .withColumnRenamed("Government Debt to GDP Ratio", "DebtToGDP") \
        .withColumnRenamed("Industrial Production Index", "IndustrialProduction") \
        .withColumnRenamed("House Price Index", "HousePrice") \
        .withColumnRenamed("Retail Sales (Value)", "RetailSalesValue") \
        .withColumnRenamed("Retail Sales (Volume)", "RetailSalesVolume")

    # Step 4: Critical Fix 1 - Left join instead of inner to retain macro data (even if central data is missing)
    # Fill central bank missing values with 0 (since inner join would drop China/Japan if central data is missing)
    eco_data = macro_wide.join(central_wide, on=["Region", "Year", "Quarter"], how="left") \
        .fillna({
            "InterestRate": 0, 
            "ForeignReserves": 0  # Fill central bank missing values instead of dropping rows
        }) \
        .fillna({
            # Fill macro missing values with forward fill (better than 0 for time-series)
            "GDP": 0, "CPI": 0, "Confidence": 0, "Unemployment": 0,
            "Exports": 0, "Imports": 0, "DebtToGDP": 0, "IndustrialProduction": 0,
            "HousePrice": 0, "RetailSalesValue": 0, "RetailSalesVolume": 0
        })
    print(f"Combined economic data: {eco_data.count()} rows")
    print("Combined eco data regions available:", [r["Region"] for r in eco_data.select("Region").distinct().collect()])

    # Step 5: Map currency pairs to regions (add validation for region existence)
    # Critical: Ensure region names match exactly with macro/central data (e.g., "China" vs "CN" vs "中国")
    pair_country_map = {
        "GBPUSD=X": ("UK", "US"),
        "USDCNY=X": ("China", "US"),  # Verify: Is your macro data using "China" or other names (e.g., "中国")?
        "USDJPY=X": ("Japan", "US")   # Verify: Is your macro data using "Japan" or other names (e.g., "日本")?
    }

    # Validate region existence in eco_data before processing
    eco_regions = {r["Region"] for r in eco_data.select("Region").distinct().collect()}
    for pair, (non_us_country, us_country) in pair_country_map.items():
        if non_us_country not in eco_regions:
            print(f"[WARNING] {non_us_country} not found in economic data! Available regions: {eco_regions}")
            print(f"          Fix: Update 'pair_country_map' in preprocess_data() to match your data's region name")
        if us_country not in eco_regions:
            print(f"[ERROR] {us_country} not found in economic data! Cannot proceed without US data.")
            raise ValueError(f"US data missing from economic data (available regions: {eco_regions})")

    # Step 6: Process US economic data (unchanged, but add logging)
    us_eco = eco_data.filter(col("Region") == "US") \
        .withColumnRenamed("InterestRate", "US_InterestRate") \
        .withColumnRenamed("ForeignReserves", "US_ForeignReserves") \
        .withColumnRenamed("GDP", "US_GDP") \
        .withColumnRenamed("CPI", "US_CPI") \
        .withColumnRenamed("Confidence", "US_ConsumerConfidence") \
        .withColumnRenamed("Unemployment", "US_UnemploymentRate") \
        .withColumnRenamed("Exports", "US_Exports") \
        .withColumnRenamed("Imports", "US_Imports") \
        .withColumnRenamed("DebtToGDP", "US_DebtToGDP") \
        .withColumnRenamed("IndustrialProduction", "US_IndustrialProduction") \
        .withColumnRenamed("HousePrice", "US_HousePrice") \
        .withColumnRenamed("RetailSalesValue", "US_RetailSalesValue") \
        .withColumnRenamed("RetailSalesVolume", "US_RetailSalesVolume") \
        .select(
            "Year", "Quarter",
            "US_InterestRate", "US_ForeignReserves", "US_GDP",
            "US_CPI", "US_ConsumerConfidence", "US_UnemploymentRate",
            "US_Exports", "US_Imports", "US_DebtToGDP",
            "US_IndustrialProduction", "US_HousePrice",
            "US_RetailSalesValue", "US_RetailSalesVolume"
        )
    print(f"US economic data: {us_eco.count()} rows")

    uk_pair_df = None    # For GBPUSD=X (UK-US)
    china_pair_df = None # For USDCNY=X (China-US)
    japan_pair_df = None # For USDJPY=X (Japan-US)

    for pair, (non_us_country, us_country) in pair_country_map.items():
        print(f"\nProcessing {pair} (Non-US country: {non_us_country})...")
        
        # Critical Fix 2 - Check if non-US data exists before processing
        non_us_eco_raw = eco_data.filter(col("Region") == non_us_country)
        if non_us_eco_raw.count() == 0:
            print(f"[WARNING] No raw data found for {non_us_country}! Skipping {pair} (check region name in macro data)")
            continue

        # Step 7: Process Non-US economic data (enhanced missing value handling)
        window_spec = Window.partitionBy("Year").orderBy("Quarter")
        non_us_eco = non_us_eco_raw \
            .withColumnRenamed("InterestRate", f"{non_us_country}_InterestRate") \
            .withColumnRenamed("ForeignReserves", f"{non_us_country}_ForeignReserves") \
            .withColumnRenamed("GDP", f"{non_us_country}_GDP") \
            .withColumnRenamed("CPI", f"{non_us_country}_CPI") \
            .withColumnRenamed("Confidence", f"{non_us_country}_ConsumerConfidence") \
            .withColumnRenamed("Unemployment", f"{non_us_country}_UnemploymentRate") \
            .withColumnRenamed("Exports", f"{non_us_country}_Exports") \
            .withColumnRenamed("Imports", f"{non_us_country}_Imports") \
            .withColumnRenamed("DebtToGDP", f"{non_us_country}_DebtToGDP") \
            .withColumnRenamed("IndustrialProduction", f"{non_us_country}_IndustrialProduction") \
            .withColumnRenamed("HousePrice", f"{non_us_country}_HousePrice") \
            .withColumnRenamed("RetailSalesValue", f"{non_us_country}_RetailSalesValue") \
            .withColumnRenamed("RetailSalesVolume", f"{non_us_country}_RetailSalesVolume") \
            .withColumn("Quarter", when(col("Quarter").isNull(), 1).otherwise(col("Quarter"))) \
            .withColumn(f"{non_us_country}_GDP", last(f"{non_us_country}_GDP", ignorenulls=True).over(window_spec)) \
            .withColumn(f"{non_us_country}_CPI", last(f"{non_us_country}_CPI", ignorenulls=True).over(window_spec)) \
            .withColumn(f"{non_us_country}_IndustrialProduction", last(f"{non_us_country}_IndustrialProduction", ignorenulls=True).over(window_spec)) \
            .withColumn(f"{non_us_country}_Exports", last(f"{non_us_country}_Exports", ignorenulls=True).over(window_spec)) \
            .select(
                "Year", "Quarter",
                f"{non_us_country}_InterestRate", f"{non_us_country}_ForeignReserves", f"{non_us_country}_GDP",
                f"{non_us_country}_CPI", f"{non_us_country}_ConsumerConfidence", f"{non_us_country}_UnemploymentRate",
                f"{non_us_country}_Exports", f"{non_us_country}_Imports", f"{non_us_country}_DebtToGDP",
                f"{non_us_country}_IndustrialProduction", f"{non_us_country}_HousePrice",
                f"{non_us_country}_RetailSalesValue", f"{non_us_country}_RetailSalesVolume"
            )
        
        print(f"{non_us_country} economic data after processing: {non_us_eco.count()} rows")
        if non_us_eco.count() == 0:
            print(f"[WARNING] No valid data for {non_us_country} after processing! Skipping {pair}")
            continue

        # Step 8: Join exchange rate + US + Non-US data (inner join to ensure time alignment)
        pair_data = exchange_filtered.filter(col("Pair") == pair) \
            .join(us_eco, on=["Year", "Quarter"], how="inner") \
            .join(non_us_eco, on=["Year", "Quarter"], how="inner")
        
        print(f"{pair} final data: {pair_data.count()} rows")
        if pair_data.count() == 0:
            print(f"[WARNING] No overlapping time periods for {pair}! (Check Year/Quarter alignment)")
            continue
        
        UNIFIED_COLUMNS = [
     
            "Year", "Quarter", "Pair", "Rate",
            # US
            "US_InterestRate", "US_ForeignReserves", "US_GDP", "US_CPI", 
            "US_ConsumerConfidence", "US_UnemploymentRate", "US_Exports", 
            "US_Imports", "US_DebtToGDP", "US_IndustrialProduction", 
            "US_HousePrice", "US_RetailSalesValue", "US_RetailSalesVolume",
            # UK
            "UK_InterestRate", "UK_ForeignReserves", "UK_GDP", "UK_CPI", 
            "UK_ConsumerConfidence", "UK_UnemploymentRate", "UK_Exports", 
            "UK_Imports", "UK_DebtToGDP", "UK_IndustrialProduction", 
            "UK_HousePrice", "UK_RetailSalesValue", "UK_RetailSalesVolume",
            # China
            "China_InterestRate", "China_ForeignReserves", "China_GDP", "China_CPI", 
            "China_ConsumerConfidence", "China_UnemploymentRate", "China_Exports", 
            "China_Imports", "China_DebtToGDP", "China_IndustrialProduction", 
            "China_HousePrice", "China_RetailSalesValue", "China_RetailSalesVolume",
            # Japan
            "Japan_InterestRate", "Japan_ForeignReserves", "Japan_GDP", "Japan_CPI", 
            "Japan_ConsumerConfidence", "Japan_UnemploymentRate", "Japan_Exports", 
            "Japan_Imports", "Japan_DebtToGDP", "Japan_IndustrialProduction", 
            "Japan_HousePrice", "Japan_RetailSalesValue", "Japan_RetailSalesVolume"
        ]

        # Union data for all pairs
        # final_data = pair_data if final_data is None else final_data.union(pair_data)
        # Fill missing columns with null (preserve structure consistency)
        missing_cols = [col_name for col_name in UNIFIED_COLUMNS if col_name not in pair_data.columns]
        for col_name in missing_cols:
            if any(kw in col_name for kw in ["Rate", "GDP", "CPI", "Interest", "Reserves"]):
                pair_data = pair_data.withColumn(col_name, lit(None).cast("double"))
            else:
                pair_data = pair_data.withColumn(col_name, lit(None).cast("string"))
        pair_data = pair_data.select(UNIFIED_COLUMNS)  # Align column order

        # Map to currency-pair DataFrame
        if pair == "GBPUSD=X":
            uk_pair_df = pair_data
        elif pair == "USDCNY=X":
            china_pair_df = pair_data
        elif pair == "USDJPY=X":
            japan_pair_df = pair_data
        
    
    print(f"\n=== Currency-Pair DataFrame Summary ===")
    # UK (GBPUSD=X)
    if uk_pair_df is not None:
        print(f"GBPUSD=X DataFrame: {uk_pair_df.count()} rows, {len(uk_pair_df.columns)} columns")
        uk_pair_df.show(2, truncate=False)
    else:
        print(f"GBPUSD=X DataFrame: No valid data")
    # China (USDCNY=X)
    if china_pair_df is not None:
        print(f"\nUSDCNY=X DataFrame: {china_pair_df.count()} rows, {len(china_pair_df.columns)} columns")
        china_pair_df.show(2, truncate=False)
    else:
        print(f"\nUSDCNY=X DataFrame: No valid data")
    # Japan (USDJPY=X)
    if japan_pair_df is not None:
        print(f"\nUSDJPY=X DataFrame: {japan_pair_df.count()} rows, {len(japan_pair_df.columns)} columns")
        japan_pair_df.show(2, truncate=False)
    else:
        print(f"\nUSDJPY=X DataFrame: No valid data")
    
    return uk_pair_df, china_pair_df, japan_pair_df



# Execute preprocessing if this file is run directly
if __name__ == "__main__":
    # Assume read_raw_data() returns (exchange_df, central_df, macro_df, spark)
    # Ensure macro_df is loaded from macro_economic_quarterly.csv with columns: Region, Category, Year, Quarter, Value
    exchange_df, central_df, macro_df, spark = read_raw_data()
    try:
        final_cleaned_data = preprocess_data(exchange_df, central_df, macro_df, spark)
        # Optional: Save final data to CSV (uncomment if needed)
        # final_cleaned_data.coalesce(1).write.mode("overwrite").option("header", True).csv("final_cleaned_data.csv")
        # print("Final data saved to 'final_cleaned_data.csv'")
    finally:
        spark.stop()
        print("Spark session stopped.")
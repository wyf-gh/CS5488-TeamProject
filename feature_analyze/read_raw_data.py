from pyspark.sql import SparkSession
import os

def read_raw_data(data_dir: str = "/cleaned_data") -> tuple:
    """
    Read raw CSV data (exchange rate, central bank, macroeconomic)
    :param data_dir: Path to cleaned_data folder (parent of feature_analyze)
    :return: Tuple of 3 DataFrames (exchange_df, central_df, macro_df)
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ReadRawData") \
        .master("local[*]") \
        .getOrCreate()

    # Define file paths
    file_paths = {
        "exchange": os.path.join(data_dir, "exchange_rate_quarterly.csv"),
        "central": os.path.join(data_dir, "central_bank_quarterly.csv"),
        "macro": os.path.join(data_dir, "macro_economic_quarterly.csv")
    }

    # Check file existence
    for file_name, file_path in file_paths.items():
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}. Please check the path.")

    # Read CSV files
    exchange_df = spark.read.csv(file_paths["exchange"], header=True, inferSchema=True)
    central_df = spark.read.csv(file_paths["central"], header=True, inferSchema=True)
    macro_df = spark.read.csv(file_paths["macro"], header=True, inferSchema=True)

    # Print basic info
    print(f"  Raw data read successfully!")
    print(f"- Exchange rate data rows: {exchange_df.count()}, columns: {len(exchange_df.columns)}")
    print(f"- Central bank data rows: {central_df.count()}, columns: {len(central_df.columns)}")
    print(f"- Macroeconomic data rows: {macro_df.count()}, columns: {len(macro_df.columns)}")

    return exchange_df, central_df, macro_df, spark

# Run if this file is executed directly
if __name__ == "__main__":
    exchange_df, central_df, macro_df, spark = read_raw_data()
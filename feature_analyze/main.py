import os
import shutil


def main():
    # Create output directory if not exists
    input_dir = "cleaned_data"
    output_dir = "feature_analysis_results"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Step 1: Read raw data
    from read_raw_data import read_raw_data
    exchange_df, central_df, macro_df, spark = read_raw_data(data_dir=input_dir)

    # Step 2: Preprocess data (MODIFIED: Receive 3 currency-pair DataFrames)
    from data_preprocessing import preprocess_data
    uk_df, china_df, japan_df = preprocess_data(exchange_df, central_df, macro_df, spark)  # Updated return value

    # Step 3: Define currency-pair mapping (for subdirs/naming)
    pair_data_map = {
        "gbpusd": ("GBPUSD=X", uk_df),
        "usdcn": ("USDCNY=X", china_df),
        "usdjpy": ("USDJPY=X", japan_df)
    }

    # Step 4: Correlation analysis (MODIFIED: Process each pair separately)
    from correlation_analysis import calculate_correlation_matrix, visualize_correlation_matrix
    
    for pair_key, (pair_name, pair_df) in pair_data_map.items():
        # Skip if DataFrame is empty/null
        if pair_df is None or pair_df.count() == 0:
            print(f"\n[WARNING] No valid data for {pair_name} - skipping correlation analysis")
            continue

        # Create subdirectory for current pair (e.g., feature_analysis_results/gbpusd)
        pair_output_dir = os.path.join(output_dir, pair_key)
        if os.path.exists(pair_output_dir):
            shutil.rmtree(pair_output_dir)  # Overwrite if exists
        os.makedirs(pair_output_dir)

        # Calculate correlation matrix (pass pair name for context)
        print(f"\nCalculating correlation matrix for {pair_name}...")
        corr_df = calculate_correlation_matrix(pair_df, pair_name=pair_name)  # Updated function call

        # Visualize correlation matrix (save to pair-specific dir)
        print(f"Visualizing correlation matrix for {pair_name}...")
        visualize_correlation_matrix(
            corr_df, 
            output_dir=pair_output_dir, 
            pair_name=pair_name  # Pass pair name for file/title customization
        )

    # Step 4: Regression analysis
    from regression_analysis import regression_analysis, visualize_regression_results
    
    regression_results_dict = {}
    for pair_key, (pair_name, pair_df) in pair_data_map.items():
        if pair_df is None or pair_df.count() == 0:
            print(f"\n[WARNING] No valid data for {pair_name} - skipping regression analysis")
            continue

        pair_output_dir = os.path.join(output_dir, pair_key)
        try:
            print(f"\nRunning regression analysis for {pair_name}...")
            reg_results = regression_analysis(
                pair_df=pair_df,
                pair_name=pair_name,
                output_dir=pair_output_dir
            )
            regression_results_dict[pair_name] = reg_results
        except Exception as e:
            print(f"\n[ERROR] Regression failed for {pair_name}: {str(e)}")

    # Visualize regression results
    if regression_results_dict:
        print("\nVisualizing regression results...")
        visualize_regression_results(
            results_dict=regression_results_dict,
            output_dir=output_dir
        )
    else:
        print("\nNo valid regression results to visualize")


    # # Step 5: Visualization
    # from visualization import run_visualization
    # run_visualization(final_data, corr_df, regression_results, output_dir=output_dir)

    # Stop SparkSession
    spark.stop()
    print("\n All analysis steps completed successfully!")
    print(f"- Results and plots are saved in: {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    main()
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame
import os
import seaborn as sns


def calculate_correlation_matrix(pair_df: DataFrame, pair_name: str) -> pd.DataFrame:
    """
    Generate correlation matrix for a SINGLE currency-pair DataFrame (matched to 3 pairs: GBPUSD=X/USDCNY=X/USDJPY=X)
    Core logic retained: Auto-clean empty rows/cols + TOP 5 US + TOP 5 non-US indicators
    Args:
        pair_df: Independent Spark DataFrame for one currency pair (e.g., uk_df for GBPUSD=X)
        pair_name: Explicit currency pair name (must be "GBPUSD=X", "USDCNY=X", or "USDJPY=X")
    Returns:
        Pandas DataFrame of correlation matrix for the target pair
    """
    # 1. Currency pair ↔ indicator prefix mapping (matches 3 target pairs)
    pair_prefix_map = {
        "GBPUSD=X": ["US_", "UK_"],    # UK pair (GBPUSD=X)
        "USDCNY=X": ["US_", "China_"], # China pair (USDCNY=X)
        "USDJPY=X": ["US_", "Japan_"]  # Japan pair (USDJPY=X)
    }

    # Validate input pair (ensure it's one of the 3 target pairs)
    if pair_name not in pair_prefix_map:
        raise ValueError(f"Unsupported currency pair: {pair_name}. Only accept 'GBPUSD=X', 'USDCNY=X', 'USDJPY=X'")

    # Validate required columns (no "Pair" column needed now—pair_df is already filtered)
    exclude_cols = {"Year", "Quarter"}  # Remove "Pair" from exclude (not in independent pair_df)
    candidate_cols = [c for c in pair_df.columns if c not in exclude_cols]
    if not candidate_cols:
        raise ValueError(f"Pair {pair_name}: No valid columns left after excluding {exclude_cols}")

    # Get US/non-US prefixes for current pair
    us_prefix, cp_prefix = pair_prefix_map[pair_name]
    cp_country = cp_prefix.rstrip("_")  # For readable logs (e.g., "China" instead of "China_")

    # 2. Filter indicators: Only US + current non-US indicators (e.g., US_ + China_ for USDCNY=X)
    valid_indicator_cols = [
        col for col in candidate_cols
        if col.startswith(us_prefix) or col.startswith(cp_prefix)
    ]
    if not valid_indicator_cols:
        raise ValueError(f"Pair {pair_name}: No indicators found (expected prefixes: {us_prefix}, {cp_prefix})")

    # 3. Auto-clean data (no need to filter by "Pair"—pair_df is independent)
    pair_pdf = (pair_df
                .select(valid_indicator_cols)  # Keep only relevant indicators
                .toPandas()                    # Spark → Pandas for correlation
                .dropna(axis=0, how="any")     # Remove rows with ANY missing values
                .dropna(axis=1, how="all"))    # Remove empty columns (e.g., all-NaN)

    # Validate cleaned data volume
    if pair_pdf.shape[0] < 2:
        raise ValueError(f"Pair {pair_name}: Insufficient rows (need ≥2, got {pair_pdf.shape[0]})")
    if pair_pdf.shape[1] < 2:
        raise ValueError(f"Pair {pair_name}: Insufficient indicators (need ≥2, got {pair_pdf.shape[1]})")

    # 4. Helper: Get TOP-K indicators by max absolute correlation (retained logic)
    def top_k_by_maxcorr(corr_abs, cols, k):
        if not cols or k <= 0:
            return []
        column_scores = {}
        for col in cols:
            if col not in corr_abs.index:
                continue
            # Exclude self-correlation + drop NaNs
            corr_values = corr_abs.loc[col].drop(labels=[col], errors="ignore").dropna()
            if not corr_values.empty:
                column_scores[col] = corr_values.max()
        # Sort descending + return top-K
        return [col for col, _ in sorted(column_scores.items(), key=lambda x: x[1], reverse=True)[:k]]

    # 5. Calculate absolute correlation matrix (for indicator ranking)
    corr_abs_matrix = pair_pdf.corr().abs()

    # 6. Separate US/non-US indicators from cleaned data
    us_indicators_clean = [col for col in pair_pdf.columns if col.startswith(us_prefix)]
    cp_indicators_clean = [col for col in pair_pdf.columns if col.startswith(cp_prefix)]

    # Critical: Ensure non-US indicators exist (e.g., no China indicators for USDCNY=X)
    if not cp_indicators_clean:
        raise ValueError(f"Pair {pair_name}: No {cp_country} indicators left after cleaning (check raw data)")

    # 7. Select TOP 5 US + TOP 5 non-US indicators
    top_5_us = top_k_by_maxcorr(corr_abs_matrix, us_indicators_clean, 5)
    top_5_cp = top_k_by_maxcorr(corr_abs_matrix, cp_indicators_clean, 5)

    # Log if fewer than 5 indicators are available
    if len(top_5_us) < 5:
        print(f"[Warning] {pair_name}: Only {len(top_5_us)} US indicators available (expected 5)")
    if len(top_5_cp) < 5:
        print(f"[Warning] {pair_name}: Only {len(top_5_cp)} {cp_country} indicators available (expected 5)")

    # 8. Merge + deduplicate + supplement to 10 indicators (retained logic)
    selected_indicators = list(dict.fromkeys(top_5_us + top_5_cp))  # Deduplicate
    if len(selected_indicators) < 10:
        # First supplement with remaining non-US indicators
        remaining_cp = [col for col in cp_indicators_clean if col not in selected_indicators]
        for col in top_k_by_maxcorr(corr_abs_matrix, remaining_cp, len(remaining_cp)):
            selected_indicators.append(col)
            if len(selected_indicators) >= 10:
                break
        # Then supplement with remaining US indicators (if still needed)
        if len(selected_indicators) < 10:
            remaining_us = [col for col in us_indicators_clean if col not in selected_indicators]
            for col in top_k_by_maxcorr(corr_abs_matrix, remaining_us, len(remaining_us)):
                selected_indicators.append(col)
                if len(selected_indicators) >= 10:
                    break

    # 9. Generate final SIGNED correlation matrix
    final_corr_matrix = pair_pdf[selected_indicators].corr()

    # 10. Log results (verify matching)
    cp_count_final = len([col for col in selected_indicators if col.startswith(cp_prefix)])
    us_count_final = len([col for col in selected_indicators if col.startswith(us_prefix)])
    empty_cols_removed = [col for col in valid_indicator_cols if col not in pair_pdf.columns]

    print(f"\n[OK] {pair_name} Correlation Matrix Generated:")
    print(f"  - Indicators: Total {len(selected_indicators)} (US: {us_count_final}, {cp_country}: {cp_count_final})")
    print(f"  - Empty Columns Removed: {empty_cols_removed if empty_cols_removed else 'None'}")
    print(f"  - Selected Indicators: {selected_indicators}")

    return final_corr_matrix


def visualize_correlation_matrix(corr_df: pd.DataFrame, output_dir: str, pair_name: str) -> None:
    """
    Visualize correlation matrix for a single currency pair (e.g., GBPUSD=X)
    Args:
        corr_df: Pandas DataFrame of correlation matrix (from calculate_correlation_matrix)
        output_dir: Directory to save the plot (pair-specific subdir from main.py)
        pair_name: Name of the currency pair (e.g., "GBPUSD=X") for filename/title customization
    """
    # Ensure output directory exists (redundant but safe)
    os.makedirs(output_dir, exist_ok=True)

    # 1. Custom filename (use pair_name to avoid overwriting, e.g., "Corr_Matrix_GBPUSD_X.png")
    # Replace "=" with "_" because "=" is invalid in some file systems
    safe_pair_name = pair_name.replace("=", "_")
    plot_filename = f"Correlation_Matrix_{safe_pair_name}.png"
    plot_path = os.path.join(output_dir, plot_filename)

    # 2. Create heatmap with pair-specific title
    plt.figure(figsize=(12, 10))  # Adjust size based on number of indicators
    sns.heatmap(
        corr_df,
        annot=True,          # Show correlation values
        fmt=".2f",           # 2 decimal places for readability
        cmap="coolwarm",     # Color scale (red=positive, blue=negative)
        vmin=-1, vmax=1,     # Fix scale to full correlation range (-1 to 1)
        annot_kws={"size": 8},  # Smaller text for dense matrices
        cbar_kws={"label": "Correlation Coefficient (-1 to 1)"},
        linewidths=0.5       # Add borders between cells
    )

    # 3. Pair-specific title (clear which currency pair the plot belongs to)
    plt.title(
        f"Correlation Matrix - {pair_name}\n(Macroeconomic Indicators)",
        fontsize=14,
        pad=20  # Space between title and plot
    )

    # 4. Improve label readability (rotate x-labels to avoid overlap)
    plt.xticks(rotation=45, ha="right", fontsize=9)
    plt.yticks(rotation=0, fontsize=9)

    # 5. Save plot (high DPI for clarity; bbox_inches fixes label cutoff)
    plt.tight_layout()
    plt.savefig(plot_path, dpi=300, bbox_inches="tight")
    print(f"[INFO] Correlation plot saved to: {plot_path}")

    # 6. Close plot to free memory (critical for multiple pairs)
    plt.close()


if __name__ == "__main__":
    """
    Test flow (matches 3 independent currency-pair DataFrames):
    1. Load raw data → Preprocess to get 3 independent pair DataFrames
    2. Calculate correlation matrix for each pair
    3. Visualize all matrices
    """
    try:
        # Step 1: Load + preprocess to get 3 independent pair DataFrames (match your actual preprocess output)
        print("=== Step 1: Load Raw Data ===")
        from read_raw_data import read_raw_data
        from data_preprocessing import preprocess_data  # This returns (uk_df, china_df, japan_df)
        exchange_df, central_df, macro_df, spark = read_raw_data(data_dir="cleaned_data")

        print("\n=== Step 2: Preprocess to Get 3 Independent Pairs ===")
        uk_df, china_df, japan_df = preprocess_data(exchange_df, central_df, macro_df, spark)  # 3 independent DataFrames

        # Step 3: Calculate correlation matrix for each pair
        print("\n=== Step 3: Calculate Correlation Matrices ===")
        corr_dict = {}
        # UK pair (GBPUSD=X)
        if uk_df is not None and uk_df.count() > 0:
            corr_dict["GBPUSD=X"] = calculate_correlation_matrix(uk_df, "GBPUSD=X")
        else:
            print("[Skip] GBPUSD=X: No valid data")
        # China pair (USDCNY=X)
        if china_df is not None and china_df.count() > 0:
            corr_dict["USDCNY=X"] = calculate_correlation_matrix(china_df, "USDCNY=X")
        else:
            print("[Skip] USDCNY=X: No valid data")
        # Japan pair (USDJPY=X)
        if japan_df is not None and japan_df.count() > 0:
            corr_dict["USDJPY=X"] = calculate_correlation_matrix(japan_df, "USDJPY=X")
        else:
            print("[Skip] USDJPY=X: No valid data")

        # Step 4: Visualize
        print("\n=== Step 4: Visualize Correlation Matrices ===")
        if corr_dict:
            visualize_correlation_matrix(corr_dict, output_dir="feature_analysis_results")
        else:
            print("No valid correlation matrices to visualize")

        # Cleanup
        spark.stop()
        print("\n=== Step 5: Spark Session Stopped ===")

    except Exception as e:
        print(f"\n[Error] Test Flow Failed: {str(e)}")
        if 'spark' in locals() and not spark.sparkContext.isStopped():
            spark.stop()
            print("Spark Session Stopped Due to Error")
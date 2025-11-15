# regression_analysis.py (FULL FIXED VERSION)
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.preprocessing import StandardScaler
import os

# ----------------------
# Fixed: Accepts parameters from main.py (pair_df, pair_name, output_dir)
# ----------------------
def regression_analysis(
    pair_df: DataFrame,          # From main.py: Single currency-pair DF (e.g., uk_df)
    pair_name: str,              # From main.py: Currency pair name (e.g., "GBPUSD=X")
    output_dir: str,             # From main.py: Pair-specific subdir (e.g., "feature_analysis_results/gbpusd")
    target_col: str = "Rate",     # Default: Exchange rate column (unchanged)
    zero_threshold: float = 1e-6  # Threshold to filter near-zero coefficients
) -> pd.DataFrame:
    """Perform linear regression for a SINGLE currency pair (matches main.py calls)."""
    # 1. Map pair to relevant indicator prefixes (US + non-US)
    pair_feature_prefixes = {
        "GBPUSD=X": ("US_", "UK_"),
        "USDCNY=X": ("US_", "China_"),
        "USDJPY=X": ("US_", "Japan_")
    }
    us_prefix, cp_prefix = pair_feature_prefixes.get(pair_name, (None, None))
    if us_prefix is None or cp_prefix is None:
        raise ValueError(f"Unsupported pair: {pair_name}. Use 'GBPUSD=X', 'USDCNY=X', 'USDJPY=X'")

    # 2. Prepare data (no need to filter "Pair" column—pair_df is already filtered)
    pandas_df = pair_df.select(
        [target_col] + [col for col in pair_df.columns 
                       if col.startswith(us_prefix) or col.startswith(cp_prefix)]
    ).toPandas()

    # 3. Validate features
    features = [col for col in pandas_df.columns if col.startswith(us_prefix) or col.startswith(cp_prefix)]
    if not features:
        raise ValueError(f"No valid features for {pair_name} (expected {us_prefix}/{cp_prefix} prefixes)")
    
    # 4. Clean data (drop missing values)
    pandas_df_clean = pandas_df.dropna(subset=[target_col] + features)
    if len(pandas_df_clean) < 10:
        raise ValueError(f"Insufficient data for {pair_name} (needs ≥10 rows, got {len(pandas_df_clean)})")

    # 5. Train regression model
    scaler = StandardScaler()
    X = scaler.fit_transform(pandas_df_clean[features])
    y = pandas_df_clean[target_col].values

    model = LinearRegression()
    model.fit(X, y)

    # 6. Calculate metrics
    y_pred = model.predict(X)
    r2 = r2_score(y, y_pred)
    adj_r2 = 1 - ((1 - r2) * (len(y) - 1)) / (len(y) - len(features) - 1)
    mse = mean_squared_error(y, y_pred)

    # 7. Organize results
    results = []
    for feat_idx, feat_name in enumerate(features):
        coef = model.coef_[feat_idx]
        # Skip indicators with near-zero coefficients (from missing data)
        if abs(coef) < zero_threshold:
            print(f"[Filtered] {pair_name}: {feat_name} (coefficient = {coef:.6f} → near zero)")
            continue
        results.append({
            "Pair": pair_name,
            "Feature": feat_name,
            "Coefficient": coef,
            "R2": r2,
            "Adjusted_R2": adj_r2,
            "MSE": mse
        })
    
    # If no valid indicators left after filtering
    if not results:
        raise ValueError(f"No valid indicators for {pair_name} (all filtered as near-zero)")
    
    # Sort indicators by coefficient (DESCENDING: strong positive → strong negative)
    results_df = pd.DataFrame(results).sort_values(by="Coefficient", ascending=False).reset_index(drop=True)
    print(f"\nSorted results for {pair_name}:")
    print(results_df[["Feature", "Coefficient"]].head(5))  # Print top 5 indicators
    # 8. Save results to pair-specific subdir
    results_path = os.path.join(output_dir, "regression_results.csv")
    results_df.to_csv(results_path, index=False)
    print(f"[Regression] Results for {pair_name} saved to: {results_path}")

    return results_df

# ----------------------
# Fixed: Accepts results_dict (from main.py) instead of results_df
# ----------------------
def visualize_regression_results(
    results_dict: dict,          # From main.py: {pair_name: results_df, ...}
    output_dir: str,             # From main.py: Root output dir (e.g., "feature_analysis_results")
    save_prefix: str = "regression_visualizations"
) -> None:
    """Visualize results for ALL currency pairs (matches main.py's results_dict)."""
    os.makedirs(output_dir, exist_ok=True)
    plt.rcParams['font.sans-serif'] = ['Arial']
    plt.rcParams['axes.unicode_minus'] = False

    # ----------------------
    # Plot 1: Coefficient Heatmap (Per Pair)
    # ----------------------
    for pair_name, results_df in results_dict.items():
        pair_subdir = os.path.join(output_dir, pair_name.replace("=", "_").lower())
        os.makedirs(pair_subdir, exist_ok=True)

        # Pivot data for heatmap
        coef_matrix = results_df.pivot(index="Feature", columns="Pair", values="Coefficient")
        # Reverse index to place largest positive coefficients at the top
        coef_matrix = coef_matrix.reindex(results_df["Feature"])

        plt.figure(figsize=(10, 8))
        im = plt.imshow(coef_matrix.values, cmap="RdBu_r", aspect="auto")
        
        plt.xticks([0], [pair_name], fontsize=10)
        plt.yticks(np.arange(len(coef_matrix.index)), coef_matrix.index, fontsize=9)
        for i in range(len(coef_matrix.index)):
            plt.text(0, i, f"{coef_matrix.iloc[i, 0]:.3f}", 
                     ha="center", va="center", color="black", fontsize=8)
        
        plt.title(f"Feature Impact on Exchange Rate: {pair_name}", fontsize=12, pad=15)
        plt.colorbar(im, shrink=0.8, label="Standardized Coefficient")
        plt.tight_layout()

        heatmap_path = os.path.join(pair_subdir, f"{save_prefix}_coefficients.png")
        plt.savefig(heatmap_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"[Visualization] Coefficient heatmap for {pair_name} saved to: {heatmap_path}")

    # ----------------------
    # Plot 2: R² Comparison (Across All Pairs)
    # ----------------------
    r2_data = []
    for pair_name, results_df in results_dict.items():
        # Extract unique R²/Adjusted R² per pair
        pair_metrics = results_df.drop_duplicates(subset=["Pair"])[["Pair", "R2", "Adjusted_R2"]].iloc[0]
        r2_data.append(pair_metrics)
    r2_df = pd.DataFrame(r2_data)

    if not r2_df.empty:
        plt.figure(figsize=(10, 6))
        x = np.arange(len(r2_df["Pair"]))
        width = 0.35

        # Plot side-by-side bars
        bars1 = plt.bar(x - width/2, r2_df["R2"], width, label="R² Score", color="skyblue")
        bars2 = plt.bar(x + width/2, r2_df["Adjusted_R2"], width, label="Adjusted R²", color="lightcoral")

        # Customize plot
        plt.xlabel("Currency Pair", fontsize=11)
        plt.ylabel("Score (0 = No Fit, 1 = Perfect Fit)", fontsize=11)
        plt.title("Model Fit Comparison Across Currency Pairs", fontsize=13, pad=15)
        plt.xticks(x, r2_df["Pair"])
        plt.legend()
        plt.ylim(0, 1)  # R² ranges 0–1 for readability

        # Add value labels
        def add_labels(bars):
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                        f"{height:.3f}", ha="center", va="bottom", fontsize=9)
        add_labels(bars1)
        add_labels(bars2)

        # Save to root output dir
        r2_path = os.path.join(output_dir, f"{save_prefix}_r2_comparison.png")
        plt.tight_layout()
        plt.savefig(r2_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"[Visualization] R² comparison chart saved to: {r2_path}")
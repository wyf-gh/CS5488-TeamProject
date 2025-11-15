import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
import os
from typing import Dict
from read_raw_data import read_raw_data
from data_preprocessing import preprocess_data
from correlation_analysis import run_correlation_analysis
from regression_analysis import run_regression_analysis


def plot_correlation_heatmap(corr_df: pd.DataFrame, output_dir: str = ".") -> None:
    """Plot correlation heatmap for each currency pair"""
    plt.figure(figsize=(18, 6))
    target_pairs = corr_df.index.tolist()

    for i, pair in enumerate(target_pairs, 1):
        plt.subplot(1, 3, i)
        pair_corr = corr_df.loc[pair].dropna().values.reshape(-1, 1)
        pair_indicators = corr_df.loc[pair].dropna().index.tolist()

        sns.heatmap(pair_corr, annot=True, cmap="RdBu_r", vmin=-1, vmax=1,
                    xticklabels=["Correlation with Rate"], yticklabels=pair_indicators,
                    cbar_kws={"shrink": 0.8})
        plt.title(f"Currency Pair: {pair}", fontsize=12)

    plt.tight_layout()
    output_path = os.path.join(output_dir, "correlation_heatmap.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"- Correlation heatmap saved to: {output_path}")

def plot_regression_coefficients(regression_results: Dict, output_dir: str = ".") -> None:
    """Plot regression coefficients for each currency pair"""
    plt.figure(figsize=(22, 7))
    target_pairs = list(regression_results.keys())

    for i, pair in enumerate(target_pairs, 1):
        plt.subplot(1, 3, i)
        result = regression_results[pair]
        coef_df = pd.DataFrame(list(result["coefficients"].items()), columns=["Indicator", "Coefficient"]) \
            .sort_values("Coefficient", key=abs, ascending=False)

        colors = ["red" if x < 0 else "green" for x in coef_df["Coefficient"]]
        sns.barplot(x="Coefficient", y="Indicator", data=coef_df, palette=colors, alpha=0.8)
        plt.title(f"{pair}\nR2={result['R2']} | RMSE={result['RMSE']}", fontsize=11)
        plt.axvline(x=0, color="black", linestyle="--", linewidth=0.8)

    plt.tight_layout()
    output_path = os.path.join(output_dir, "regression_coefficients.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"- Regression coefficients plot saved to: {output_path}")

def plot_residuals(final_data: DataFrame, regression_results: Dict, output_dir: str = ".") -> None:
    """Plot residual plots to verify model fit"""
    pair_country_map = {
        "GBPUSD=X": ("United Kingdom", "United States"),
        "USDCNY=X": ("China", "United States"),
        "USDJPY=X": ("Japan", "United States")
    }
    label_col = "Rate"

    def get_feature_cols(pair: str) -> list:
        non_us_country = pair_country_map[pair][0]
        return [
            "US_PolicyRate", "US_ForeignReserves", "US_GDP", "US_CPI", "US_Confidence",
            f"{non_us_country}_PolicyRate", f"{non_us_country}_ForeignReserves",
            f"{non_us_country}_GDP", f"{non_us_country}_CPI", f"{non_us_country}_Confidence"
        ]

    plt.figure(figsize=(20, 6))
    target_pairs = list(regression_results.keys())

    for i, pair in enumerate(target_pairs, 1):
        plt.subplot(1, 3, i)
        pair_data = final_data.filter(col("Pair") == pair)
        feature_cols = get_feature_cols(pair)
        feature_cols = [col for col in feature_cols if col in pair_data.columns]

        # Rebuild model to get predictions
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="rawFeatures")
        data_with_features = assembler.transform(pair_data)
        scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
        scaler_model = scaler.fit(data_with_features)
        scaled_data = scaler_model.transform(data_with_features)
        lr = LinearRegression(featuresCol="features", labelCol=label_col, maxIter=100, regParam=0.01)
        lr_model = lr.fit(scaled_data)
        predictions = lr_model.transform(scaled_data).toPandas()

        # Calculate residuals
        predictions["Residuals"] = predictions["Rate"] - predictions["prediction"]
        sns.scatterplot(x="prediction", y="Residuals", data=predictions, alpha=0.6, color="blue")
        plt.axhline(y=0, color="red", linestyle="--", linewidth=1)
        plt.title(f"Currency Pair: {pair}", fontsize=11)
        plt.xlabel("Predicted Exchange Rate")
        plt.ylabel("Residuals (Actual - Predicted)")

    plt.tight_layout()
    output_path = os.path.join(output_dir, "regression_residuals.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"- Residual plots saved to: {output_path}")

def run_visualization(final_data: DataFrame, corr_df: pd.DataFrame, regression_results: Dict, output_dir: str = ".") -> None:
    """Run all visualizations and save plots"""
    print(" Starting visualization...")
    # Set plot style
    sns.set_style("whitegrid")
    plt.rcParams['axes.unicode_minus'] = False

    # Generate all plots
    plot_correlation_heatmap(corr_df, output_dir)
    plot_regression_coefficients(regression_results, output_dir)
    plot_residuals(final_data, regression_results, output_dir)

    print(" Visualization completed! All plots saved.")

# Run if this file is executed directly
if __name__ == "__main__":


    exchange_df, central_df, macro_df, spark = read_raw_data()
    final_data = preprocess_data(exchange_df, central_df, macro_df, spark)
    corr_df = run_correlation_analysis(final_data)
    regression_results = run_regression_analysis(final_data)
    run_visualization(final_data, corr_df, regression_results)
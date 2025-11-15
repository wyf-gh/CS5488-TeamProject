# fx_analysis_quarterly.py
import os
import glob
import shutil
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, quarter, avg, round, to_date, lag, stddev, mean
from pyspark.sql.functions import expr
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml import Pipeline

import pandas as pd
import matplotlib.pyplot as plt

# -------------------------
# 0. Configuration
# -------------------------
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# Replace these paths if needed
base_dir = os.path.abspath("cleaned_data")
exchange_path = os.path.join(base_dir, "exchange_quarterly.csv")
central_path = os.path.join(base_dir, "central_quarterly.csv")
macro_path = os.path.join(base_dir, "macro_quarterly.csv")

output_dir = os.path.abspath("analysis_output")
os.makedirs(output_dir, exist_ok=True)

# Currency pairs to analyze (example: three pairs)
pairs_to_analyze = ["USDJPY", "GBPUSD", "USDCNY"]  # change to your actual three pairs

# -------------------------
# 1. Start Spark
# -------------------------
spark = SparkSession.builder \
    .appName("FX_Macro_Central_Analysis") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# -------------------------
# 2. Load data
# -------------------------
exchange = spark.read.csv(exchange_path, header=True, inferSchema=True)
central = spark.read.csv(central_path, header=True, inferSchema=True)
macro = spark.read.csv(macro_path, header=True, inferSchema=True)

# Ensure consistent column names: assume central has Region, DataType, Indicator, Year, Quarter, Value
# macro likewise.
# Exchange: Pair, Year, Quarter, Avg_Open, Avg_Close, Avg_Change

# -------------------------
# 3. Pivot central and macro data into wide format per Region
# -------------------------
def pivot_indicators(df, region_col="Region"):
    # pivot on Indicator, average Value (should be one per quarter per indicator but safe)
    pivoted = df.groupBy(region_col, "Year", "Quarter") \
                .pivot("Indicator") \
                .avg("Value")
    return pivoted

central_wide = pivot_indicators(central, "Region").cache()
macro_wide = pivot_indicators(macro, "Region").cache()

# -------------------------
# 4. Helper: get region codes from pair like 'GBPUSD'
# -------------------------
def split_pair(pair):
    # Assume 6-letter like GBPUSD or USDJPY: first 3 base, last 3 quote
    pair = pair.strip()
    if len(pair) == 6:
        return pair[0:3], pair[3:6]
    # fallback: try underscore separator
    if "_" in pair:
        parts = pair.split("_")
        return parts[0], parts[1]
    raise ValueError(f"Cannot parse pair: {pair}")

# -------------------------
# 5. For each pair: prepare features, train models, evaluate, plot
# -------------------------
results_summary = []

for pair in pairs_to_analyze:
    print("\nProcessing pair:", pair)
    base_code, quote_code = split_pair(pair)

    # Join exchange with base and quote macro/central data by Year/Quarter
    exch_pair = exchange.filter(col("Pair") == pair).select("Year", "Quarter", "Avg_Change")

    # get base indicators
    base_central = central_wide.filter(col("Region") == base_code).drop("Region")
    base_macro = macro_wide.filter(col("Region") == base_code).drop("Region")

    # get quote indicators
    quote_central = central_wide.filter(col("Region") == quote_code).drop("Region")
    quote_macro = macro_wide.filter(col("Region") == quote_code).drop("Region")

    # join base and quote indicator tables with prefixed column names
    def prefix_columns(df, prefix):
        for c in df.columns:
            if c not in ("Year", "Quarter"):
                df = df.withColumnRenamed(c, f"{prefix}_{c}")
        return df

    bcentral_pref = prefix_columns(base_central, "bcentral")
    bmacro_pref = prefix_columns(base_macro, "bmacro")
    qcentral_pref = prefix_columns(quote_central, "qcentral")
    qmacro_pref = prefix_columns(quote_macro, "qmacro")

    # Join everything
    joined = exch_pair \
        .join(bcentral_pref, ["Year", "Quarter"], how="left") \
        .join(bmacro_pref, ["Year", "Quarter"], how="left") \
        .join(qcentral_pref, ["Year", "Quarter"], how="left") \
        .join(qmacro_pref, ["Year", "Quarter"], how="left") \
        .orderBy("Year", "Quarter")

    # -------------------------
    # 6. Create lag features and rolling volatility (using window)
    # -------------------------
    # create window ordered by Year,Quarter (we build a synthetic index to order)
    # convert Year & Quarter to a single ordering number: period_index = Year*4 + Quarter
    joined = joined.withColumn("period_index", (col("Year") * 4) + col("Quarter"))

    win = Window.orderBy("period_index").rowsBetween(-4, -1)  # past 4 quarters (lag window)

    # lag of Avg_Change (1 quarter)
    joined = joined.withColumn("lag1_change", lag("Avg_Change", 1).over(win.rowsBetween(-1, -1)))
    # rolling volatility (stddev over past 4 quarters)
    joined = joined.withColumn("vol_4q", stddev("Avg_Change").over(win))

    # We will also include the immediate previous change as a feature (use lag with offset 1)
    win1 = Window.orderBy("period_index")
    joined = joined.withColumn("lag1_change_exact", lag("Avg_Change", 1).over(win1))

    # -------------------------
    # 7. Prepare ML dataset: drop rows with null label or null period_index
    # -------------------------
    ml_df = joined.filter(col("Avg_Change").isNotNull() & col("period_index").isNotNull())

    # Select feature columns dynamically (all columns except Year,Quarter,Avg_Change,period_index)
    exclude = set(["Year", "Quarter", "Avg_Change", "period_index"])
    feature_cols = [c for c in ml_df.columns if c not in exclude]

    # For safety, drop non-numeric columns in features (if any)
    # Identify numeric columns via schema
    numeric_features = []
    schema = ml_df.schema
    for f in feature_cols:
        dt = schema[f].dataType.simpleString() if f in schema.names else None
        if dt and dt in ("double", "int", "long", "float", "decimal"):
            numeric_features.append(f)
    # Also include lag1_change_exact and vol_4q if present
    for extra in ["lag1_change_exact", "vol_4q"]:
        if extra in numeric_features:
            pass
        elif extra in feature_cols:
            numeric_features.append(extra)

    feature_cols = [c for c in numeric_features if c != "Avg_Change"]  # final features

    if not feature_cols:
        print("No numeric features available for pair", pair)
        continue

    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

    # Scale features for linear model
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)

    # Prepare final dataset
    assembled = assembler.transform(ml_df).cache()
    scaled = scaler.fit(assembled).transform(assembled)

    final = scaled.select("features", col("Avg_Change").alias("label"), "Year", "Quarter", "period_index")

    # Train/test split: time-series split (train earlier periods)
    total_periods = final.select("period_index").distinct().count()
    # compute split index (80% earliest)
    period_values = [r[0] for r in final.select("period_index").distinct().orderBy("period_index").collect()]
    split_index = int(len(period_values) * 0.8)
    if split_index < 1:
        split_index = 1
    cutoff = period_values[split_index - 1]

    train = final.filter(col("period_index") <= cutoff)
    test = final.filter(col("period_index") > cutoff)

    # If test empty, use 70/30 arbitrary split on rows
    if test.count() == 0:
        train, test = final.randomSplit([0.8, 0.2], seed=42)

    # -------------------------
    # 8. Linear regression (elasticNet=1 => Lasso)
    # -------------------------
    lr = LinearRegression(featuresCol="features", labelCol="label", elasticNetParam=1.0, regParam=0.1, maxIter=100)
    lr_model = lr.fit(train)

    train_summary = lr_model.summary
    lr_r2 = train_summary.r2
    lr_rmse = train_summary.rootMeanSquaredError

    # Coefficients (note these are per scaled features)
    lr_coeffs = lr_model.coefficients.toArray().tolist()

    # -------------------------
    # 9. Random forest
    # -------------------------
    rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=100, maxDepth=5)
    rf_model = rf.fit(train)

    # Predict on test
    pred_train = rf_model.transform(train)
    pred_test = rf_model.transform(test)

    # Evaluate (use simple metrics computed locally)
    from pyspark.ml.evaluation import RegressionEvaluator
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    rf_rmse = evaluator.evaluate(pred_test)
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    rf_r2 = evaluator_r2.evaluate(pred_test)

    # -------------------------
    # 10. Feature importances mapping
    # -------------------------
    rf_importances = rf_model.featureImportances.toArray()
    feat_imp = list(zip(feature_cols, rf_importances))
    feat_imp_sorted = sorted(feat_imp, key=lambda x: x[1], reverse=True)

    # Save feature importances to CSV
    fi_df = pd.DataFrame(feat_imp_sorted, columns=["feature", "importance"])
    fi_csv = os.path.join(output_dir, f"{pair}_feature_importances.csv")
    fi_df.to_csv(fi_csv, index=False)
    print("Feature importances saved to:", fi_csv)

    # Linear coefficients mapping (note: coefficients correspond to scaled features)
    coeff_df = pd.DataFrame({
        "feature": feature_cols,
        "lr_coefficient": lr_coeffs
    }).sort_values(by="lr_coefficient", key=lambda s: s.abs(), ascending=False)
    coeff_csv = os.path.join(output_dir, f"{pair}_linear_coefficients.csv")
    coeff_df.to_csv(coeff_csv, index=False)
    print("Linear coefficients saved to:", coeff_csv)

    # -------------------------
    # 11. Predictions and plotting
    # -------------------------
    # Get test predictions as pandas for plotting
    pred_test_pd = pred_test.select("period_index", "label", "prediction").orderBy("period_index").toPandas()

    # Actual vs predicted plot
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(pred_test_pd["period_index"], pred_test_pd["label"], label="Actual")
    ax.plot(pred_test_pd["period_index"], pred_test_pd["prediction"], label="Predicted")
    ax.set_title(f"{pair}: Actual vs Predicted Avg_Change (test)")
    ax.set_xlabel("period_index (Year*4 + Quarter)")
    ax.set_ylabel("Avg_Change")
    ax.legend()
    plt.tight_layout()
    out_plot1 = os.path.join(output_dir, f"{pair}_actual_vs_predicted.png")
    fig.savefig(out_plot1)
    plt.close(fig)
    print("Saved plot:", out_plot1)

    # Feature importance bar chart (top 20)
    topn = fi_df.sort_values("importance", ascending=False).head(20)
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.barh(topn["feature"][::-1], topn["importance"][::-1])
    ax2.set_title(f"{pair}: Top Features (Random Forest importance)")
    ax2.set_xlabel("Importance")
    plt.tight_layout()
    out_plot2 = os.path.join(output_dir, f"{pair}_feature_importance.png")
    fig2.savefig(out_plot2)
    plt.close(fig2)
    print("Saved plot:", out_plot2)

    # -------------------------
    # 12. Save metrics summary
    # -------------------------
    summary = {
        "pair": pair,
        "lr_r2_train": lr_r2,
        "lr_rmse_train": lr_rmse,
        "rf_r2_test": rf_r2,
        "rf_rmse_test": rf_rmse,
        "n_train": train.count(),
        "n_test": test.count(),
        "num_features": len(feature_cols)
    }
    results_summary.append(summary)

    # Save predictions for inspection
    preds_csv = os.path.join(output_dir, f"{pair}_predictions_test.csv")
    pred_test.select("period_index", "label", "prediction").orderBy("period_index").toPandas().to_csv(preds_csv, index=False)
    print("Saved test predictions to:", preds_csv)

# -------------------------
# 13. Save results summary
# -------------------------
summary_df = pd.DataFrame(results_summary)
summary_csv = os.path.join(output_dir, "models_summary.csv")
summary_df.to_csv(summary_csv, index=False)
print("Saved models summary to:", summary_csv)

# -------------------------
# 14. Stop Spark safely
# -------------------------
spark.stop()
print("Spark stopped.")

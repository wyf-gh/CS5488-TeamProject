import pandas as pd
import matplotlib.pyplot as plt
import os


# ------------------------------------------------------
# Step 1: Load and Preprocess Data
# ------------------------------------------------------

df = pd.read_csv("cleaned_data/exchange_rate_quarterly.csv")
df["Quarter_Label"] = df["Year"].astype(str) + "Q" + df["Quarter"].astype(str)
df = df.sort_values(by=["Pair", "Year", "Quarter"]).reset_index(drop=True)
output_dir = "exchange_rate_quarterly_reports"

# ------------------------------------------------------
# Step 2: Plot 1 - Cross-Currency Volatility Comparison
# ------------------------------------------------------
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(14, 8))

pair_styles = {
    "GBPUSD=X": {"color": "#1f77b4", "marker": "o", "label": "GBPUSD"},
    "USDCNY=X": {"color": "#ff7f0e", "marker": "s", "label": "USDCNY"},
    "USDJPY=X": {"color": "#2ca02c", "marker": "^", "label": "USDJPY"}
}

for pair, style in pair_styles.items():
    pair_data = df[df["Pair"] == pair]
    ax.plot(
        pair_data["Quarter_Label"],
        pair_data["Avg_Change"],
        color=style["color"],
        marker=style["marker"],
        linewidth=2.5,
        markersize=7,
        label=style["label"]
    )

ax.axhline(y=0, color="gray", linestyle="--", alpha=0.6, linewidth=1.5)
ax.set_title("Quarterly Exchange Rate Volatility Comparison (2020-2024)", fontsize=16, pad=20, fontweight='bold')
ax.set_xlabel("Quarter", fontsize=12, fontweight='bold')
ax.set_ylabel("Average Price Change (Avg_Change)", fontsize=12, fontweight='bold')

# Correct way to set x-tick: fontsize (plt.xticks) + rotation/ha (text properties)
plt.xticks(fontsize=10)  # Only set fontsize here
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")  # Set rotation/ha for text
ax.tick_params(axis="y", labelsize=10)  # y-tick label size (valid: labelsize)

ax.legend(loc="upper right", fontsize=11, frameon=True, fancybox=True, shadow=True)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "cross_currency_volatility_report.png"), dpi=300, bbox_inches="tight")
plt.close()

# ------------------------------------------------------
# Step 3: Plot 2 - Price Trend + Volatility Overlay
# ------------------------------------------------------
current_pairs = df["Pair"].unique()

for target_pair in current_pairs:
    pair_data = df[df["Pair"] == target_pair]

    fig, ax1 = plt.subplots(figsize=(14, 8))

    # Plot Avg_Open/Avg_Close (price trend)
    ax1.plot(
        pair_data["Quarter_Label"],
        pair_data["Avg_Open"],
        color="#ff7f0e",
        marker="o",
        linewidth=2.5,
        markersize=7,
        label=f"{target_pair} Avg Open"
    )
    ax1.plot(
        pair_data["Quarter_Label"],
        pair_data["Avg_Close"],
        color="#1f77b4",
        marker="s",
        linewidth=2.5,
        markersize=7,
        label=f"{target_pair} Avg Close"
    )

    ax1.set_xlabel("Quarter", fontsize=12, fontweight='bold')
    ax1.set_ylabel("Exchange Rate Price", fontsize=12, fontweight='bold', color="#1f77b4")
    ax1.tick_params(axis="y", labelcolor="#1f77b4", labelsize=10)  # Valid: labelcolor + labelsize

    # Correct x-tick setup (no ha/rotation in tick_params())
    ax1.tick_params(axis="x", labelsize=10)  # Only label size here
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha="right")  # Rotate + align text

    ax1.legend(loc="upper left", fontsize=11, frameon=True)
    ax1.grid(True, alpha=0.3)

    # Plot Volatility (bar chart)
    ax2 = ax1.twinx()
    bars = ax2.bar(
        pair_data["Quarter_Label"],
        pair_data["Avg_Change"],
        color=pair_data["Avg_Change"].apply(lambda x: "#2ca02c" if x >= 0 else "#d62728"),
        alpha=0.7,
        width=0.6
    )

    ax2.set_ylabel("Quarterly Volatility (Avg_Change)", fontsize=12, fontweight='bold', color="#d62728")
    ax2.tick_params(axis="y", labelcolor="#d62728", labelsize=10)
    ax2.axhline(y=0, color="gray", linestyle="--", alpha=0.6, linewidth=1.5)

    ax1.set_title(f"{target_pair} Quarterly Price Trend & Volatility (2020-2024)", fontsize=16, pad=20, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{target_pair}_price_volatility_report.png"), dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Generated report for {target_pair}: {target_pair}_price_volatility_report.png")

print("All reports generated successfully.")

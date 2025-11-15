import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ------------------------------------------------------
# Step 1: Load and Preprocess Data
# ------------------------------------------------------
df = pd.read_csv("cleaned_data/exchange_rate_quarterly.csv")  # Replace with your data path
df["Quarter_Label"] = df["Year"].astype(str) + "Q" + df["Quarter"].astype(str)
df = df.sort_values(by=["Pair", "Year", "Quarter"]).reset_index(drop=True)
output_dir = "plots/exchange_rate_plots"
# ------------------------------------------------------
# Step 2: Plot 1 - Interactive Cross-Currency Volatility
# ------------------------------------------------------
fig = go.Figure()

pair_styles = {
    "GBPUSD=X": {"color": "#1f77b4", "marker": "circle", "label": "GBPUSD"},
    "USDCNY=X": {"color": "#ff7f0e", "marker": "square", "label": "USDCNY"},
    "USDJPY=X": {"color": "#2ca02c", "marker": "triangle-up", "label": "USDJPY"}
}

for pair, style in pair_styles.items():
    pair_data = df[df["Pair"] == pair]
    fig.add_trace(go.Scatter(
        x=pair_data["Quarter_Label"],
        y=pair_data["Avg_Change"],
        name=style["label"],
        line=dict(color=style["color"], width=3),
        marker=dict(symbol=style["marker"], size=8, line=dict(width=1, color="white")),
        hoverinfo="x+y+name",
        hovertemplate=(
            "<b>Quarter:</b> %{x}<br>"
            "<b>Volatility (Avg_Change):</b> %{y:.6f}<br>"
            "<b>Currency Pair:</b> %{name}<br>"
            "<extra></extra>"
        )
    ))

fig.add_hline(
    y=0,
    line_dash="dash",
    line_color="gray",
    line_width=2,
    annotation_text="Volatility Balance (y=0)",
    annotation_position="right",
    annotation_font=dict(size=10, weight="bold")
)

# Correct title padding (use a dictionary)
fig.update_layout(
    title=dict(
        text="Quarterly Exchange Rate Volatility Comparison (2020-2024)",
        font_size=18,
        font_weight="bold",
        pad=dict(b=20, t=20)  # Use dict for padding (bottom/top)
    ),
    xaxis=dict(
        title="Quarter",
        title_font=dict(size=14, weight="bold"),
        tickangle=45,
        tickfont=dict(size=11)
    ),
    yaxis=dict(
        title="Average Price Change (Avg_Change)",
        title_font=dict(size=14, weight="bold"),
        tickfont=dict(size=11)
    ),
    legend=dict(
        x=1.02,
        y=1,
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor="gray",
        borderwidth=1,
        font=dict(size=12)
    ),
    hovermode="x unified",
    plot_bgcolor="white",
    margin=dict(l=50, r=100, t=80, b=50)
)


fig.write_html(f"{output_dir}/cross_currency_volatility_presentation.html")
fig.show()

# ------------------------------------------------------
# Step 3: Plot 2 - Interactive Price + Volatility Overlay (Loop for All Pairs)
# ------------------------------------------------------
currency_pairs = df["Pair"].unique()
for target_pair in currency_pairs:
    pair_data = df[df["Pair"] == target_pair].copy()

    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]],
        subplot_titles=[f"{target_pair} Quarterly Price Trend & Volatility (2020-2024)"]
    )

    fig.add_trace(
        go.Scatter(
            x=pair_data["Quarter_Label"],
            y=pair_data["Avg_Open"],
            name=f"{target_pair} Avg Open",
            line=dict(color="#ff7f0e", width=3),
            marker=dict(symbol="circle", size=8),
            hovertemplate=(
                "<b>Quarter:</b> %{x}<br>"
                "<b>Avg Open Price:</b> %{y:.4f}<br>"
                "<extra></extra>"
            )
        ),
        row=1, col=1, secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=pair_data["Quarter_Label"],
            y=pair_data["Avg_Close"],
            name=f"{target_pair} Avg Close",
            line=dict(color="#1f77b4", width=3),
            marker=dict(symbol="square", size=8),
            hovertemplate=(
                "<b>Quarter:</b> %{x}<br>"
                "<b>Avg Close Price:</b> %{y:.4f}<br>"
                "<extra></extra>"
            )
        ),
        row=1, col=1, secondary_y=False
    )

    fig.add_trace(
        go.Bar(
            x=pair_data["Quarter_Label"],
            y=pair_data["Avg_Change"],
            name=f"{target_pair} Volatility",
            marker_color=pair_data["Avg_Change"].apply(lambda x: "#2ca02c" if x >= 0 else "#d62728"),
            opacity=0.7,
            hovertemplate=(
                "<b>Quarter:</b> %{x}<br>"
                "<b>Volatility (Avg_Change):</b> %{y:.6f}<br>"
                "<extra></extra>"
            )
        ),
        row=1, col=1, secondary_y=True
    )

    fig.update_xaxes(
        title_text="Quarter",
        title_font=dict(size=14, weight="bold"),
        tickangle=45,
        tickfont=dict(size=11),
        row=1, col=1
    )

    fig.update_yaxes(
        title_text="Exchange Rate Price",
        title_font=dict(size=14, weight="bold", color="#1f77b4"),
        tickfont=dict(size=11, color="#1f77b4"),
        secondary_y=False,
        row=1, col=1
    )

    fig.update_yaxes(
        title_text="Quarterly Volatility (Avg_Change)",
        title_font=dict(size=14, weight="bold", color="#d62728"),
        tickfont=dict(size=11, color="#d62728"),
        secondary_y=True,
        row=1, col=1
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="gray",
        line_width=2,
        annotation_text="Volatility Balance",
        annotation_position="right",
        secondary_y=True,
        row=1, col=1
    )

    # Correct title padding here as well
    fig.update_layout(
        title=dict(
            font_size=18,
            font_weight="bold",
            pad=dict(b=20, t=20)
        ),
        legend=dict(
            x=1.05,
            y=1,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="gray",
            borderwidth=1,
            font=dict(size=12)
        ),
        hovermode="x unified",
        plot_bgcolor="white",
        margin=dict(l=50, r=120, t=80, b=50)
    )

    fig.write_html(f"{output_dir}/price_volatility_overlay_{target_pair.replace('=X','')}.html")
    fig.show()

    print(f"Generated plot for {target_pair}")

print("All interactive plots generated successfully!")
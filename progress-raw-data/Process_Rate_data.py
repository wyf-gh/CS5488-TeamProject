"""
处理汇率数据：日度→月度聚合，异常值处理与可视化
数据频率：日度（Daily）→月度（Monthly）
数据字段：开盘价（Open），最高价（High），最低价（Low），收盘价（Close）
数据时间范围：2020-01-01 至 2024-12-31
"""


import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

def process_daily_to_monthly(file_path):
    """读取OHLC数据，跳过前3行，聚合为月度数据"""
    # 跳过前3行无效内容，手动设置列名并解析日期
    df = pd.read_csv(
        file_path,
        skiprows=3,  # 跳过前3行无效表头
        names=['Date', 'Open', 'High', 'Low', 'Close'],  # 手动指定列名
        parse_dates=['Date'],  # 解析日期列
        index_col='Date'  # 将日期设为索引
    )
    
    # 提取货币对名称（如USDCNY=X→USDCNY）
    currency_pair = file_path.stem.split('_')[0].replace('=X', '')
    
    # 按月聚合：Open取月初值，High取月内最大值，Low取月内最小值，Close取月末值
    monthly_df = df.resample('M').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    }).reset_index()
    
    monthly_df['Currency_Pair'] = currency_pair
    return monthly_df

def detect_outliers(df, col, method='zscore', threshold=3):
    """异常值检测与标记（Z-score或IQR方法）"""
    if method == 'zscore':
        z = (df[col] - df[col].mean()) / df[col].std()
        df['Outlier'] = np.abs(z) > threshold
    elif method == 'iqr':
        q1, q3 = df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df['Outlier'] = (df[col] < lower) | (df[col] > upper)
    return df

def visualize_rate_trends_with_dual_y(combined_df, img_dir):
    """使用双Y轴展示不同量级的货币对"""
    plt.figure(figsize=(12, 6))
    ax1 = plt.gca()  # 主Y轴（GBPUSD、USDCNY）
    ax2 = ax1.twinx()  # 次Y轴（USDJPY）
    
    # 绘制主Y轴货币对（GBPUSD、USDCNY）
    for pair in ['GBPUSD', 'USDCNY']:
        subset = combined_df[combined_df['Currency_Pair'] == pair]
        ax1.plot(subset['Date'], subset['Close'], label=pair)
    
    # 绘制次Y轴货币对（USDJPY）
    usdjpy = combined_df[combined_df['Currency_Pair'] == 'USDJPY']
    ax2.plot(usdjpy['Date'], usdjpy['Close'], color='green', label='USDJPY')
    
    # 设置轴标签与图例
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Exchange Rate (GBPUSD, USDCNY)', color='blue')
    ax2.set_ylabel('Exchange Rate (USDJPY)', color='green')
    
    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.title('Monthly Exchange Rates (2020-2024) - Dual Y-Axis')
    plt.savefig(os.path.join(img_dir, 'exchange_rate_trends_dual_y.png'))
    plt.close()

def visualize_rate_trends_with_subplots(combined_df, img_dir):
    """使用子图分别展示每个货币对"""
    pairs = combined_df['Currency_Pair'].unique()
    fig, axes = plt.subplots(len(pairs), 1, figsize=(12, 8), sharex=True)
    
    for i, pair in enumerate(pairs):
        subset = combined_df[combined_df['Currency_Pair'] == pair]
        axes[i].plot(subset['Date'], subset['Close'])
        axes[i].set_title(f'{pair} Monthly Exchange Rate')
        axes[i].set_ylabel('Exchange Rate')
    
    axes[-1].set_xlabel('Date')
    plt.tight_layout()
    plt.savefig(os.path.join(img_dir, 'exchange_rate_trends_subplots.png'))
    plt.close()

def visualize_rate_correlation(combined_df, img_dir):
    """绘制货币对相关性热力图"""
    pivot_df = combined_df.pivot(index='Date', columns='Currency_Pair', values='Close')
    corr = pivot_df.corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Correlation Between Currency Pairs')
    plt.savefig(os.path.join(img_dir, 'rate_correlation_heatmap.png'))
    plt.close()

if __name__ == '__main__':
    rate_dir = Path('exchange_rate_data')
    rate_files = list(rate_dir.glob('*.csv'))
    save_img_dir = 'rate_analysis_images'
    # 1. 日度→月度聚合（含所有OHLC列）
    monthly_rates = []
    for file in rate_files:
        monthly_df = process_daily_to_monthly(file)
        monthly_rates.append(monthly_df)
    combined_rates = pd.concat(monthly_rates, ignore_index=True)

    # 2. 异常值处理（按货币对分别检测）
    for pair in combined_rates['Currency_Pair'].unique():
        subset = combined_rates[combined_rates['Currency_Pair'] == pair]
        combined_rates.loc[subset.index, :] = detect_outliers(subset, 'Close', method='zscore')

    # 3. 可视化
    visualize_rate_trends_with_dual_y(combined_rates, save_img_dir)
    visualize_rate_trends_with_subplots(combined_rates, save_img_dir)
    visualize_rate_correlation(combined_rates, save_img_dir)

    print("汇率数据处理完成！已生成趋势图和相关性热力图。")
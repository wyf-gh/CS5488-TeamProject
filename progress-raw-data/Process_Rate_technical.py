"""
处理汇率数据，生成技术指标并进行简化版可视化
技术指标包括布林带（Bollinger Bands）和KDJ指标
"""



import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def process_ohlc_with_indicators(file_path):
    """处理OHLC数据，生成布林带、KDJ等技术指标"""
    df = pd.read_csv(
        file_path,
        skiprows=3,  # 跳过前3行无效表头
        names=['Date', 'Open', 'High', 'Low', 'Close'],  # 手动指定列名
        parse_dates=['Date'],  # 解析日期列
        index_col='Date'  # 将日期设为索引
    )
    currency_pair = file_path.stem.split('_')[0].replace('=X', '')
    
    # ===== 1. 布林带计算（20日周期） =====
    df['close_ma20'] = df['Close'].rolling(window=20).mean()
    df['close_std20'] = df['Close'].rolling(window=20).std()
    df['upper_band'] = df['close_ma20'] + 2 * df['close_std20']
    df['lower_band'] = df['close_ma20'] - 2 * df['close_std20']
    df['bband_break_upper'] = df['High'] > df['upper_band']
    df['bband_break_lower'] = df['Low'] < df['lower_band']

    # ===== 2. KDJ计算（9日周期） =====
    df['low_9'] = df['Low'].rolling(window=9).min()
    df['high_9'] = df['High'].rolling(window=9).max()
    df['rsv'] = (df['Close'] - df['low_9']) / (df['high_9'] - df['low_9']) * 100
    df['K'] = df['rsv'].ewm(span=3, adjust=False).mean()
    df['D'] = df['K'].ewm(span=3, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    df['kdj_overbought'] = df['J'] > 80
    df['kdj_oversold'] = df['J'] < 20
    df['kdj_gold_cross'] = (df['K'].shift(1) < df['D'].shift(1)) & (df['K'] > df['D'])
    df['kdj_death_cross'] = (df['K'].shift(1) > df['D'].shift(1)) & (df['K'] < df['D'])

    df['Currency_Pair'] = currency_pair
    return df

def visualize_simplified_indicators(df, currency_pair, img_dir):
    """简化版可视化：减少冗余标记，优化配色与布局"""
    plt.figure(figsize=(12, 10), dpi=150)  # 提高DPI增强清晰度

    # 子图1：布林带
    ax1 = plt.subplot(2, 1, 1)
    ax1.plot(df.index, df['Close'], label='Close', linewidth=1.2, color='#1f77b4')
    ax1.plot(df.index, df['close_ma20'], label='20-day MA', linewidth=1, color='#ff7f0e')
    ax1.plot(df.index, df['upper_band'], linestyle='--', color='#9467bd', alpha=0.7)
    ax1.plot(df.index, df['lower_band'], linestyle='--', color='#9467bd', alpha=0.7)
    # 仅标记显著突破点（过滤频繁波动）
    ax1.scatter(
        df[df['bband_break_upper']].index, 
        df[df['bband_break_upper']]['High'], 
        color='red', marker='^', s=50, label='Upper Band Break', alpha=0.7
    )
    ax1.scatter(
        df[df['bband_break_lower']].index, 
        df[df['bband_break_lower']]['Low'], 
        color='green', marker='v', s=50, label='Lower Band Break', alpha=0.7
    )
    ax1.set_title(f'Bollinger Bands - {currency_pair}')
    ax1.set_ylabel('Exchange Rate')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.3)

    # 子图2：KDJ
    ax2 = plt.subplot(2, 1, 2, sharex=ax1)
    ax2.plot(df.index, df['K'], label='K', linewidth=1, color='#1f77b4')
    ax2.plot(df.index, df['D'], label='D', linewidth=1, color='#d62728')
    ax2.plot(df.index, df['J'], label='J', linewidth=1.5, color='#2ca02c')
    ax2.axhline(y=80, linestyle='--', color='gray', alpha=0.5)
    ax2.axhline(y=20, linestyle='--', color='gray', alpha=0.5)
    # 仅标记极端超买超卖和关键交叉
    ax2.scatter(
        df[df['kdj_overbought']].index, 
        df[df['kdj_overbought']]['J'], 
        color='red', marker='*', s=60, label='Overbought', alpha=0.7
    )
    ax2.scatter(
        df[df['kdj_oversold']].index, 
        df[df['kdj_oversold']]['J'], 
        color='green', marker='*', s=60, label='Oversold', alpha=0.7
    )
    ax2.scatter(
        df[df['kdj_gold_cross']].index, 
        df[df['kdj_gold_cross']]['K'], 
        color='green', marker='^', s=50, label='Gold Cross', alpha=0.7
    )
    ax2.scatter(
        df[df['kdj_death_cross']].index, 
        df[df['kdj_death_cross']]['K'], 
        color='red', marker='v', s=50, label='Death Cross', alpha=0.7
    )
    ax2.set_title(f'KDJ Indicator - {currency_pair}')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('KDJ Value')
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.3)

    plt.tight_layout()
    # 确保图片保存到指定文件夹
    if not Path(img_dir).exists():
        Path(img_dir).mkdir()
    plt.savefig(f'{img_dir}/{currency_pair}_simplified_indicators.png')
    plt.close()

if __name__ == '__main__':
    img_dir = 'Rate_data_images'  # 图片保存文件夹
    rate_dir = Path('exchange_rate_data')
    rate_files = list(rate_dir.glob('*.csv'))

    for file in rate_files:
        df = process_ohlc_with_indicators(file)
        currency_pair = df['Currency_Pair'].iloc[0]
        visualize_simplified_indicators(df, currency_pair, img_dir)

    print(f"简化版技术指标图已保存至 {img_dir} 文件夹")
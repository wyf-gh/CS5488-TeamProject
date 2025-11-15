import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

def process_wb_wide_to_long(file_path):
    """将世界银行宽表（年份为列）转换为长表（年份为行）"""
    df = pd.read_csv(file_path)
    # 熔融为长表
    df_long = df.melt(
        id_vars=['Series Name', 'Series Code', 'Country Name', 'Country Code'],
        var_name='Year',
        value_name='Value'
    )
    # 提取纯年份（如"2020 [YR2020]"→2020）
    df_long['Year'] = df_long['Year'].str.extract('(\d{4})').astype(int)
    # 标记指标名称
    indicator = file_path.stem.replace('_', ' ').title()
    df_long['Indicator'] = indicator
    return df_long

def handle_outliers(df, col, method='iqr'):
    """异常值处理（默认IQR方法，适合宏观经济数据）"""
    if method == 'iqr':
        q1, q3 = df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df['Outlier'] = (df[col] < lower) | (df[col] > upper)
    return df

def visualize_country_indicator_trends(combined_df):
    """绘制每个国家的指标趋势图（按指标分类）"""
    indicators = combined_df['Indicator'].unique()
    for ind in indicators:
        ind_df = combined_df[combined_df['Indicator'] == ind]
        plt.figure(figsize=(12, 6))
        for country in ind_df['Country Name'].unique():
            country_df = ind_df[ind_df['Country Name'] == country]
            plt.plot(country_df['Year'], country_df['Value'], label=country)
        plt.title(f'{ind} Trend by Country (2020-2024)')
        plt.xlabel('Year')
        plt.ylabel('Value')
        plt.legend()
        plt.savefig(f'{ind.replace(" ", "_")}_trend.png')
        plt.close()

def visualize_indicator_correlation(combined_df, country_code='CHN'):
    """绘制某国指标间的相关性热力图（以中国为例）"""
    country_df = combined_df[combined_df['Country Code'] == country_code]
    pivot_df = country_df.pivot(index='Year', columns='Indicator', values='Value')
    corr = pivot_df.corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title(f'Indicator Correlation - {country_df["Country Name"].iloc[0]}')
    plt.savefig(f'{country_code}_indicator_correlation.png')
    plt.close()

if __name__ == '__main__':
    wb_dir = Path('world_bank_wdi_data')
    wb_files = list(wb_dir.glob('*.csv'))

    # 1. 宽表→长表转换
    wb_long_list = []
    for file in wb_files:
        long_df = process_wb_wide_to_long(file)
        wb_long_list.append(long_df)
    combined_wb = pd.concat(wb_long_list, ignore_index=True)

    # 2. 异常值处理（按国家+指标分别检测）
    for ind in combined_wb['Indicator'].unique():
        for country in combined_wb['Country Name'].unique():
            subset = combined_wb[(combined_wb['Indicator'] == ind) & (combined_wb['Country Name'] == country)]
            combined_wb.loc[subset.index, :] = handle_outliers(subset, 'Value', method='iqr')

    # 3. 可视化
    visualize_country_indicator_trends(combined_wb)
    visualize_indicator_correlation(combined_wb, country_code='CHN')  # 可替换为USA、GBR、JPN
    visualize_indicator_correlation(combined_wb, country_code='USA')

    print("世界银行数据处理完成！已生成各国指标趋势图和相关性热力图。")
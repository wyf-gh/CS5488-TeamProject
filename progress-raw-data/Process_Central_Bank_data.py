"""
Central Bank Data Visualization Tool (Optimized Version)
for Top 4 Economies (US, China, Japan, Eurozone)
Functionality: Unified data frequency, spread analysis, insight report
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os
import plotly.express as px  # 新增：交互式图表库，支持hover查看数值

# -------------------------- 基础配置：解决中文显示与路径问题 --------------------------
# 配置matplotlib字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STHeiti', 'WenQuanYi Zen Hei']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示异常问题

# 定义核心数据目录（使用os.path处理，兼容Windows/macOS/Linux）
DATA_DIR = "central_bank_data"
IMAGE_DIR = "central_bank_images"
if not os.path.exists(IMAGE_DIR):
    os.makedirs(IMAGE_DIR)
    print(f"[Info] Created data directory: {os.path.abspath(DATA_DIR)}")


def load_central_bank_data(file_name, freq='ME'):
    """
    加载央行数据（优化核心函数）
    功能：1.统一时间频率 2.异常值过滤 3.路径安全处理 4.错误提示
    :param file_name: 英文数据文件名（需与下载的CSV文件一致）
    :param freq: 目标时间频率，默认'M'=月度（日度数据将转为月度均值）
    :return: 清洗后的DataFrame或None（文件缺失/错误时）
    """
    # 构建跨系统安全路径（避免Windows反斜杠/ macOS正斜杠问题）
    file_path = os.path.join(DATA_DIR, file_name)
    
    # 检查文件是否存在，不存在则提示并返回
    if not os.path.exists(file_path):
        print(f"[Warning] Missing data file: {file_name} (skip this indicator)")
        return None
    
    try:
        # 1. 基础数据加载与时间格式化
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])  # 统一转为datetime类型，便于时间排序
        df = df.dropna(subset=['value']).sort_values('date')  # 删除value为空的行，按时间升序
        
        # 2. 统一时间频率：日度→月度均值，月度数据保持不变（避免图表线条密度不一致）
        # 原理：按date列重采样，取每月均值，确保所有数据频率统一
        df = df.resample(freq, on='date')['value'].mean().reset_index()
        
        # 3. 异常值过滤：用3σ原则（适合利率这类平稳数据，排除极端异常值）
        mean_val = df['value'].mean()  # 均值
        std_val = df['value'].std()    # 标准差
        # 保留均值±3倍标准差范围内的数据，过滤极端值
        df = df[(df['value'] >= mean_val - 3 * std_val) & (df['value'] <= mean_val + 3 * std_val)]
        
        return df
    
    # 捕获文件读取错误（如CSV格式错误、数据类型异常）
    except Exception as e:
        print(f"[Error] Failed to read {file_name}: {str(e)} (skip this indicator)")
        return None


def plot_policy_rates_comparison():
    """
    绘制四大经济体央行政策利率对比图（修正事件标注+中国利率显示）
    """
    print("[Process] Generating central bank policy rates comparison chart...")
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # 定义利率数据（确保文件名与下载的CSV完全一致）
    rates_data = {
        'US Fed Funds Rate': ('US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv', '#1f77b4', '-'),
        'UK Deposit Facility Rate': ('UK_Deposit_Interest_Rate_Monthly_IR3TIB01GBM156N.csv', '#ff7f0e', '-'),
        'Japan Short-Term Rate': ('Japan_Short-Term_Interest_Rate_Monthly_IRSTCI01JPM156N.csv', '#2ca02c', '-'),
        'China Deposit Rate': ('China_Deposit_Interest_Rate_Monthly_INTDSRCNM193N.csv', '#d62728', '-'),
    }
    
    # 加载并绘制每条利率曲线
    for label, (file_name, color, line_style) in rates_data.items():
        data = load_central_bank_data(file_name)
        if data is not None:
            ax.plot(
                data['date'], data['value'],
                label=label,
                color=color,
                linestyle=line_style,
                linewidth=2.5
            )
    
    # -------------------------- 修正事件标注位置（基于准确日期） --------------------------
    key_events = [
        # 美联储2022年3月首次激进加息
        ('2022-03-01', 0.2, 'Fed starts aggressive rate hikes'),
        # 英国央行2023年8月到达峰值5.53%
        ('2023-08-01', 5.5, 'BoE peaks at 5.53%'),
        # 中国央行保持利率稳定
        ('2023-09-01', 2.9, 'PBOC maintains stable rates'),
        # 日本央行2024年3月结束负利率
        ('2024-03-01', 0.2, 'BOJ ends negative rate policy'),
    ]
    
    for event_date, y_pos, event_desc in key_events:
        ax.annotate(
            event_desc,
            xy=(pd.to_datetime(event_date), y_pos),
            xytext=(10, 20),
            textcoords='offset points',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
            arrowprops=dict(arrowstyle='->', color='red', alpha=0.6),
            fontsize=9,
            fontweight='bold'
        )
    
    # -------------------------- 图表格式优化 --------------------------
    ax.set_title('Central Bank Policy Rates Comparison (2020-2024)', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Interest Rate (%)', fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11, loc='upper left')
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1.5, alpha=0.6)
    
    # X轴日期格式化（每6个月显示一次，避免拥挤）
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存图表
    output_path = os.path.join(IMAGE_DIR, 'central_bank_rates_comparison_corrected.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[Success] Saved corrected chart: {os.path.abspath(output_path)}")
    plt.close()


def plot_forex_reserves_comparison():
    """
    绘制四国外汇储备对比图（优化版）
    改进：四国数据同图展示（原版本拆分中国/美国，现整合避免信息割裂）
    输出：英文命名的PNG图表
    """
    print("[Process] Generating foreign exchange reserves comparison chart...")
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # 定义四国外汇储备数据：key=图例（英文），value=(文件名, 颜色, 标记点)
    reserves_data = {
        'China': ('China_Foreign_Exchange_Reserves_Monthly_TRESEGCNM052N.csv', '#d62728', 'o'),
        'Japan': ('Japan_Foreign_Exchange_Reserves_Monthly_TRESEGJPM052N.csv', '#2ca02c', 's'),
        'US': ('US_Foreign_Exchange_Reserves_Monthly_TRESEGUSM052N.csv', '#1f77b4', '^'),
        'UK': ('UK_Foreign_Exchange_Reserves_Monthly_TRESEGGBM052N.csv', '#ff7f0e', 'D'),
    }
    
    # 循环加载并绘制每条储备曲线
    for label, (file_name, color, marker) in reserves_data.items():
        data = load_central_bank_data(file_name)
        if data is not None:
            # 单位转换：原数据单位为百万美元→十亿美元（更易读）
            data['value_billion'] = data['value'] / 1000
            
            # 绘制储备趋势线
            ax.plot(
                data['date'], data['value_billion'],
                label=label,
                color=color,
                linewidth=2.5,
                marker=marker,        # 标记点（区分不同国家）
                markersize=5,         # 标记点大小
                alpha=0.8             # 透明度（避免重叠遮挡）
            )
            
            # 填充曲线下方区域（增强视觉层次感）
            ax.fill_between(data['date'], data['value_billion'], alpha=0.2, color=color)
    
    # 标注最新储备值（仅显示有有效数据的国家）
    for label, (file_name, color, _) in reserves_data.items():
        data = load_central_bank_data(file_name)
        if data is not None:
            latest_data = data.iloc[-1]  # 最后一行=最新数据
            latest_billion = latest_data['value'] / 1000  # 转换为十亿单位
            ax.annotate(
                f'{label}: ${latest_billion:.0f}B',  # 标注内容（国家+最新值）
                xy=(latest_data['date'], latest_billion),  # 标注位置
                xytext=(0, 10),  # 文本向上偏移10点
                textcoords='offset points',
                fontsize=9,
                fontweight='bold',
                color=color  # 文本颜色与曲线一致
            )
    
    # -------------------------- 图表格式优化 --------------------------
    ax.set_title('Foreign Exchange Reserves Comparison (2020-2024)', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Foreign Exchange Reserves (Billion USD)', fontsize=12)  # 单位：十亿美金
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11, loc='upper right')  # 图例放右上（避免遮挡中国储备曲线）
    
    # X轴日期格式（与利率图保持一致，风格统一）
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存图表
    output_path = os.path.join(IMAGE_DIR, 'forex_reserves_comparison_optimized.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[Success] Saved chart: {os.path.abspath(output_path)}")
    plt.close()


def plot_interest_spread():
    """
    新增：绘制利率利差分析图（美国vs中/欧/日）
    核心作用：衔接汇率分析需求（利差是影响汇率的关键因素）
    输出：英文命名的PNG图表
    """
    print("[Process] Generating interest rate spread analysis chart...")
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # 加载美国基准利率（作为利差计算的基准，美元是全球核心货币）
    us_rate_data = load_central_bank_data('US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv')
    if us_rate_data is None:
        print("[Error] US interest rate data missing, cannot generate spread chart")
        return
    
    # 定义利差组合：key=利差名称（英文），value=(其他国家利率文件名, 颜色)
    spread_pairs = {
        'US-China Spread': ('China_Deposit_Interest_Rate_Monthly_INTDSRCNM193N.csv', '#d62728'),
        'US-UK Spread': ('UK_Deposit_Interest_Rate_Monthly_IR3TIB01GBM156N.csv', '#ff7f0e'),
        'US-Japan Spread': ('Japan_Short-Term_Interest_Rate_Monthly_IRSTCI01JPM156N.csv', '#2ca02c'),
    }
    
    # 循环计算并绘制每条利差曲线
    for spread_label, (other_file, color) in spread_pairs.items():
        other_rate_data = load_central_bank_data(other_file)
        if other_rate_data is not None:
            # 按日期合并美国与其他国家利率数据（确保时间对齐）
            spread_df = pd.merge(
                us_rate_data, other_rate_data,
                on='date',  # 合并键：日期
                suffixes=('_us', '_other')  # 区分两表的value列
            )
            
            # 计算利差：美国利率 - 其他国家利率（利差扩大通常利好美元）
            spread_df['spread'] = spread_df['value_us'] - spread_df['value_other']
            
            # 绘制利差趋势线
            ax.plot(
                spread_df['date'], spread_df['spread'],
                label=spread_label,
                color=color,
                linewidth=2.5,
                alpha=0.8
            )
    
    # -------------------------- 图表格式优化 --------------------------
    ax.set_title('US vs Other Economies Interest Rate Spread (2020-2024)', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Interest Rate Spread (Percentage Points)', fontsize=12)  # 单位：百分点
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11, loc='upper left')
    # 零线：利差正负分界（利差>0=美国利率高于其他国家，利差<0=相反）
    ax.axhline(y=0, color='black', linestyle='-', linewidth=1, alpha=0.7)
    
    # X轴日期格式
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存图表
    output_path = os.path.join(IMAGE_DIR, 'us_interest_rate_spread.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[Success] Saved chart: {os.path.abspath(output_path)}")
    plt.close()





def plot_rate_changes_analysis():
    """
    绘制利率变化幅度分析图（优化版：水平条形图+精确数值展示）
    """
    print("[Process] Generating interest rate change analysis chart...")
    
    fig, ax = plt.subplots(figsize=(12, 8))  # 调整尺寸，适配水平布局
    
    # 定义需分析的利率文件
    rates_files = {
        'US': ('US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv', '#2ca02c'),
        'UK': ('UK_Deposit_Interest_Rate_Monthly_IR3TIB01GBM156N.csv', '#1f77b4'),
        'Japan': ('Japan_Short-Term_Interest_Rate_Monthly_IRSTCI01JPM156N.csv', '#ff7f0e'),
        'China': ('China_Deposit_Interest_Rate_Monthly_INTDSRCNM193N.csv', '#d62728'),
    }
    
    # 计算各国利率累计变化（保留两位小数）
    change_data = []
    for economy, (file_name, color) in rates_files.items():
        data = load_central_bank_data(file_name)
        if data is not None and len(data) > 1:
            start_val = data.iloc[0]['value']  # 起始值（2020年）
            end_val = data.iloc[-1]['value']   # 结束值（2024年）
            total_change = round(end_val - start_val, 2)  # 精确到两位小数
            change_data.append({
                'economy': economy,
                'total_change': total_change,
                'start_val': start_val,
                'end_val': end_val
            })
    
    # 按变化幅度排序（便于对比）
    change_data.sort(key=lambda x: x['total_change'], reverse=True)
    economies = [d['economy'] for d in change_data]
    total_changes = [d['total_change'] for d in change_data]
    # 颜色规则：加息（正变化）→绿色，降息（负变化）→红色
    bar_colors = ['#2ca02c' if change > 0 else '#d62728' for change in total_changes]
    
    # 绘制水平条形图（解决下标拥挤问题）
    bars = ax.barh(
        economies,
        total_changes,
        color=bar_colors,
        alpha=0.8,
        edgecolor='black',
        linewidth=1.5
    )
    
    # 为每个条形添加数值标注（显示变化幅度+起始/结束值）
    for bar, change, data in zip(bars, total_changes, change_data):
        # 标注位置：右侧（正变化）或左侧（负变化）
        align = 'left' if change > 0 else 'right'
        x_pos = bar.get_width() + 0.02 if change > 0 else bar.get_width() - 0.02
        
        ax.text(
            x_pos,
            bar.get_y() + bar.get_height()/2.,
            f'Change: {change:+.2f}%\nRange: {data["start_val"]:.2f}%→{data["end_val"]:.2f}%',
            ha=align,
            va='center',
            fontsize=10,
            fontweight='bold',
            linespacing=1.2
        )
    
    # -------------------------- 图表格式优化 --------------------------
    ax.axvline(x=0, color='black', linestyle='-', linewidth=1.2)  # 零线（变化正负分界）
    ax.set_title('Total Interest Rate Change (2020-2024)', 
                fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Interest Rate Change (Percentage Points)', fontsize=12)
    ax.grid(True, alpha=0.3, axis='x')  # 仅显示X轴网格
    
    # 添加说明文本（解释颜色含义）
    ax.text(
        0.02, 0.98,
        'Green = Total Rate Hikes, Red = Total Rate Cuts',
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment='top',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='wheat', alpha=0.7)
    )
    
    plt.tight_layout()
    
    # 保存图表
    output_path = os.path.join(IMAGE_DIR, 'interest_rate_change_analysis_optimized_horizontal.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[Success] Saved corrected chart: {os.path.abspath(output_path)}")
    plt.close()



def create_comprehensive_dashboard():
    """创建综合仪表板（修正布局冲突，删除重复图表）"""
    print("[Process] Generating comprehensive data dashboard...")
    
    # 创建3行2列的子图布局，明确每个子图位置
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.4, wspace=0.3)  # 增加间距避免拥挤
    
    # -------------------------- 1. 当前政策利率（1行1列） --------------------------
    ax1 = fig.add_subplot(gs[0, 0])
    rates_data = {
        'US': ('US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv', '#1f77b4'),
        'UK': ('UK_Deposit_Interest_Rate_Monthly_IR3TIB01GBM156N.csv', '#ff7f0e'),
        'Japan': ('Japan_Short-Term_Interest_Rate_Monthly_IRSTCI01JPM156N.csv', '#2ca02c'),
        'China': ('China_Deposit_Interest_Rate_Monthly_INTDSRCNM193N.csv', '#d62728'),
    }
    latest_rates = {}
    for economy, (file_name, color) in rates_data.items():
        data = load_central_bank_data(file_name)
        if data is not None:
            latest_rates[economy] = data.iloc[-1]['value']
    if latest_rates:
        economies = list(latest_rates.keys())
        values = list(latest_rates.values())
        colors_list = [rates_data[eco][1] for eco in economies]
        
        ax1.bar(economies, values, color=colors_list, alpha=0.8, edgecolor='black')
        ax1.set_title('Current Policy Interest Rates', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Interest Rate (%)', fontsize=10)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # 标注利率值
        for i, val in enumerate(values):
            ax1.text(i, val, f'{val:.2f}%', ha='center', va='bottom', fontsize=9, fontweight='bold')

    # -------------------------- 2. 外汇储备对比（1行2列） --------------------------
    ax2 = fig.add_subplot(gs[0, 1])
    reserves_files = {
        'China': ('China_Foreign_Exchange_Reserves_Monthly_TRESEGCNM052N.csv', '#d62728'),
        'Japan': ('Japan_Foreign_Exchange_Reserves_Monthly_TRESEGJPM052N.csv', '#2ca02c'),
        'US': ('US_Foreign_Exchange_Reserves_Monthly_TRESEGUSM052N.csv', '#1f77b4'),
        'UK': ('UK_Foreign_Exchange_Reserves_Monthly_TRESEGGBM052N.csv', '#ff7f0e'),
    }
    latest_reserves = {}
    for economy, (file_name, color) in reserves_files.items():
        data = load_central_bank_data(file_name)
        if data is not None:
            latest_reserves[economy] = data.iloc[-1]['value'] / 1000  # 转换为十亿美金
    if latest_reserves:
        economies = list(latest_reserves.keys())
        values = list(latest_reserves.values())
        colors_list = [reserves_files[eco][1] for eco in economies]
        
        ax2.bar(economies, values, color=colors_list, alpha=0.8, edgecolor='black')
        ax2.set_title('Latest Foreign Exchange Reserves', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Reserves (Billion USD)', fontsize=10)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # 标注储备值
        for i, val in enumerate(values):
            ax2.text(i, val, f'${val:.0f}B', ha='center', va='bottom', fontsize=9, fontweight='bold')

    # -------------------------- 3. 美联储利率走廊（2行1列） --------------------------
    ax3 = fig.add_subplot(gs[1, 0])
    us_dff = load_central_bank_data('US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv')
    us_iorb = load_central_bank_data('US_Interest_Rate_on_Reserves_Daily_IORB.csv')
    if us_dff is not None:
        ax3.plot(us_dff['date'], us_dff['value'], label='Fed Funds Rate', color='#1f77b4', linewidth=1.5)
    if us_iorb is not None:
        ax3.plot(us_iorb['date'], us_iorb['value'], label='IORB Rate', color='#ff7f0e', linewidth=1.5, linestyle='--')
    ax3.set_title('Fed Interest Rate Corridor', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Interest Rate (%)', fontsize=10)
    ax3.legend(fontsize=8)
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    # -------------------------- 4. （可选）利率与储备联动分析（示例：美国利率vs储备） --------------------------
    ax4 = fig.add_subplot(gs[1, 1])
    us_rate = load_central_bank_data('US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv')
    us_reserves = load_central_bank_data('US_Foreign_Exchange_Reserves_Monthly_TRESEGUSM052N.csv')
    if us_rate is not None and us_reserves is not None:
        # 合并数据（按日期对齐）
        merged = pd.merge(us_rate, us_reserves, on='date', how='inner', suffixes=('_rate', '_reserves'))
        merged['value_reserves'] = merged['value_reserves'] / 1000  # 十亿USD
        
        # 双轴图：左轴利率，右轴储备
        ax4_left = ax4
        ax4_right = ax4.twinx()
        ax4_left.plot(merged['date'], merged['value_rate'], label='Fed Funds Rate', color='#1f77b4', linewidth=1.5)
        ax4_right.plot(merged['date'], merged['value_reserves'], label='US Forex Reserves', color='#ff7f0e', linewidth=1.5, linestyle='--')
        
        ax4_left.set_title('US: Interest Rate vs Forex Reserves', fontsize=12, fontweight='bold')
        ax4_left.set_ylabel('Interest Rate (%)', fontsize=10)
        ax4_right.set_ylabel('Reserves (Billion USD)', fontsize=10)
        
        # 合并图例
        lines, labels = ax4_left.get_legend_handles_labels()
        lines2, labels2 = ax4_right.get_legend_handles_labels()
        ax4_right.legend(lines + lines2, labels + labels2, fontsize=8)
        ax4_left.grid(True, alpha=0.3)
        ax4_left.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    else:
        ax4.text(0.5, 0.5, 'Data not available', ha='center', va='center', fontsize=10, color='red')

    # -------------------------- 仪表板总标题 --------------------------
    fig.suptitle('Central Bank Data Dashboard (2020-2024)', 
                 fontsize=16, fontweight='bold', y=0.99)
    
    # 保存仪表板
    output_path = os.path.join(IMAGE_DIR, 'central_bank_dashboard.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[Success] Saved dashboard: {os.path.abspath(output_path)}")
    plt.close()


def main():
    """
    主函数：执行所有可视化流程
    逻辑：检查数据目录→生成各类图表→生成报告→输出完成信息
    """
    print("=" * 70)
    print("Central Bank Data Visualization Tool (Optimized Version)")
    print("Top 4 Economies: US, China, Japan, Eurozone")
    print("=" * 70)
    
    # 检查数据目录是否存在（若不存在，已在开头创建，此处仅提示）
    if not os.path.exists(DATA_DIR):
        print(f"[Error] Data directory '{DATA_DIR}' not found. Please run the download script first.")
        return
    
    print(f"\n[Info] Starting visualization process...")
    print(f"[Info] All outputs will be saved to: {os.path.abspath(DATA_DIR)}")
    
    # 1. 执行所有可视化函数（按逻辑顺序）
    plot_policy_rates_comparison()       # 利率对比图
    plot_forex_reserves_comparison()     # 外汇储备对比图
    plot_interest_spread()               # 利率利差图
    plot_rate_changes_analysis()         # 利率变化分析图
    create_comprehensive_dashboard()     # 综合仪表板
    
    
    # 2. 输出完成信息
    print("\n" + "=" * 70)
    print("All Visualization Tasks Completed Successfully!")
    print("=" * 70)
    print("\nGenerated Files List:")
    print("1. central_bank_rates_comparison_optimized.png (Policy Rates Comparison)")
    print("2. forex_reserves_comparison_optimized.png (Forex Reserves Comparison)")
    print("3. us_interest_rate_spread.png (US vs Others Rate Spread)")
    
    print("4. interest_rate_change_analysis_optimized.png (Rate Change Analysis)")
    print("5. comprehensive_central_bank_dashboard.png (Comprehensive Dashboard)")
    print("6. central_bank_data_insights_report.txt (Data Insight Report)")


# 程序入口：仅在直接运行脚本时执行
if __name__ == "__main__":
    main()
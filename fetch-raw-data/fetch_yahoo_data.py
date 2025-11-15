"""
获取雅虎数据
美国，中国，英国， 日本三国货币对的汇率数据
数据频率：日度（Daily） 
数据字段：开盘价（Open），最高价（High），最低价（Low），收盘价（Close）
数据时间范围：2020-01-01 至 2024-12-31
"""

# 2. 导入库
import yfinance as yf
import pandas as pd
from multiprocessing import Pool  # 用于并行处理
import os  # 用于创建存储目录

# 3. 配置参数
START_DATE = "2020-01-01"  # 数据起始时间（可按需调整）
END_DATE = "2024-12-31"    # 数据结束时间
CURRENCY_PAIRS = ["USDCNY=X", "GBPUSD=X", "USDJPY=X"]  # 3个目标货币对
SAVE_DIR = "exchange_rate_data"  # 存储文件夹名称（避免文件杂乱）

# 4. 创建存储文件夹（若不存在则新建）
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)
    print(f"已创建存储文件夹：{SAVE_DIR}")

# 5. 定义单货币对数据获取函数（每个进程执行此函数）
def fetch_currency_data(currency_pair):
    """
    功能：获取单个货币对的OHLC数据，存储为CSV文件
    参数：currency_pair - Yahoo Finance货币对代码（如"USDCNY=X"）
    """
    try:
        # 步骤1：下载数据（yfinance自动获取日度OHLC数据）
        data = yf.download(
            tickers=currency_pair,
            start=START_DATE,
            end=END_DATE,
            progress=False  # 关闭下载进度条（避免多进程打印混乱）
        )
        
        # 步骤2：筛选列（只保留OHLC，剔除成交量Volume和调整后收盘价Adj Close）
        data_filtered = data[["Open", "High", "Low", "Close"]]
        
        # 步骤3：定义保存路径（文件夹+货币对+时间范围）
        save_filename = f"{currency_pair}_{START_DATE.replace('-','')}_{END_DATE.replace('-','')}.csv"
        save_path = os.path.join(SAVE_DIR, save_filename)
        
        # 步骤4：保存为CSV（index=True保留日期索引，后续分析需用日期）
        data_filtered.to_csv(save_path, index=True, encoding="utf-8")
        
        # 打印进度（便于确认每个货币对是否完成）
        print(f"✅ 成功获取：{currency_pair}，文件保存至：{save_path}")
        return f"Success: {currency_pair}"
    
    except Exception as e:
        # 捕获异常（如网络错误、货币对代码错误）
        print(f"失败：{currency_pair}，错误原因：{str(e)}")
        return f"Failed: {currency_pair}"

# 6. 并行执行：用进程池处理3个货币对任务
if __name__ == "__main__":  # 多进程必须加此判断（Windows系统要求）
    print("开始并行获取汇率数据...")
    
    # 创建进程池：processes=3 表示同时启动3个进程（对应3个货币对）
    with Pool(processes=3) as pool:
        # 将CURRENCY_PAIRS列表中的每个元素传入fetch_currency_data函数，并行执行
        results = pool.map(fetch_currency_data, CURRENCY_PAIRS)
    
    # 打印最终结果
    print("\n=== 数据获取完成 ===")
    for result in results:
        print(result)
# test_import.py
import time

# 逐个测试导入，每步打印状态
print(f"[1] 开始导入pyspark（{time.strftime('%H:%M:%S')}）")
from pyspark.sql import SparkSession
print("[1] pyspark导入成功")

print(f"\n[2] 开始导入read_raw_data（{time.strftime('%H:%M:%S')}）")
from read_raw_data import read_raw_data  # 重点测试这个自定义模块
print("[2] read_raw_data导入成功")

print(f"\n[3] 开始导入data_preprocessing（{time.strftime('%H:%M:%S')}）")
from data_preprocessing import preprocess_data
print("[3] data_preprocessing导入成功")

print(f"\n[4] 开始导入correlation_analysis（{time.strftime('%H:%M:%S')}）")
from correlation_analysis import run_correlation_analysis
print("[4] correlation_analysis导入成功")

print(f"\n[5] 开始导入visualization（{time.strftime('%H:%M:%S')}）")
from visualization import run_visualization
print("[5] visualization导入成功")

print("\n✅ 所有模块导入成功！")
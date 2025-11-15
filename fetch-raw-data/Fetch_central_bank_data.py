"""
四大经济体央行数据下载工具
下载中国、美国、日本、欧元区的央行基准利率和外汇储备数据
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time
from io import StringIO

class CentralBankDataDownloader:
    """央行数据下载器"""
    
    def __init__(self):
        self.data_dir = "central_bank_data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            print(f"Created data directory: {self.data_dir}")
    
    def download_fred_series(self, series_id, series_name):
        """从FRED下载数据"""
        print(f"\nDownloading: {series_name} ({series_id})...")
        
        try:
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            df = pd.read_csv(StringIO(response.text))
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # 固定时间范围：2020年1月1日 至 2024年12月31日
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2024, 12, 31)
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            
            if not df.empty:
                filename = f"{self.data_dir}/{series_name}_{series_id}.csv"
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"✓ Successfully downloaded {len(df)} records")
                print(f"  Saved to: {filename}")
                print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
                
                # 显示最新值
                latest = df.dropna(subset=['value']).tail(1)
                if not latest.empty:
                    print(f"  Latest value: {latest.iloc[0]['value']:.4f} ({latest.iloc[0]['date'].date()})")
                
                return df
            return None
            
        except Exception as e:
            print(f"✗ Download failed: {e}")
            return None
    

    def download_all_central_bank_data(self):
        """下载所有央行数据"""
        print("=" * 70)
        print("Central Bank Data Download Tool - Top 4 Economies")
        print("=" * 70)
        print("\nData Source: FRED (Federal Reserve Economic Data)")
        print("Time Range: January 1, 2020 to December 31, 2024")
        print("\nStarting download...\n")
        
       
        data_series = [
            # ==================== 美国 ====================
            ("US_Data", [
                ("DFF", "US_Fed_Funds_Effective_Rate_Daily"),
                ("FEDFUNDS", "US_Fed_Funds_Target_Rate_Monthly"),
                ("IORB", "US_Interest_Rate_on_Reserves_Daily"),
            ]),
            
            ("US_Forex_Reserves", [
                ("TRESEGUSM052N", "US_Foreign_Exchange_Reserves_Monthly"),
            ]),
            
            # ==================== 欧元区 ====================
            ("Eurozone_Data", [
                ("ECBDFR", "Eurozone_ECB_Deposit_Facility_Rate_Daily"),
                ("ECBMRRFR", "Eurozone_ECB_Main_Refinancing_Rate_Daily"),
            ]),
            
            # ==================== 英国 ====================
            ("UK_Data", [
                ("IR3TIB01GBM156N", "UK_Deposit_Interest_Rate_Monthly"),
                
            ]),
        
            ("UK_Forex_Reserves", [
                ("TRESEGGBM052N", "UK_Foreign_Exchange_Reserves_Monthly"),
            ]),
            
             # ==================== 日本 ====================
            ("Japan_Monetary_Policy", [
                # 日本短期利率（月度）
                ("IRSTCI01JPM156N", "Japan_Short-Term_Interest_Rate_Monthly"),
                ("INTGSBJPM193N", "Japan_Government_Bond_Yield_Monthly"),
            ]),
            ("Japan_Forex_Reserves", [
                # 日本外汇储备（月度）：
                ("TRESEGJPM052N", "Japan_Foreign_Exchange_Reserves_Monthly"),
            ]),
            
            # ==================== 中国 ====================
            ("China_Data", [
                ("INTDSRCNM193N", "China_Deposit_Interest_Rate_Monthly"),
            ]),
            
            ("China_Forex_Reserves", [
                ("TRESEGCNM052N", "China_Foreign_Exchange_Reserves_Monthly"),
            ]),
            
            # ==================== 全球基准利率 ====================
            ("Global_Benchmark_Rates", [
                ("SOFR", "US_Secured_Overnight_Financing_Rate_Daily"),
                ("EFFR", "US_Effective_Federal_Funds_Rate_Daily"),
            ]),
        ]
        
        results = {}
        total_downloaded = 0
        total_attempted = 0
        
        for category, series_list in data_series:
            print("\n" + "=" * 70)
            print(f"【{category}】")
            print("=" * 70)
            
            for series_id, series_name in series_list:
                total_attempted += 1
                
                if series_id == "DISCONTINUED":
                    print(f"\n⚠️  {series_name}: No direct data available on FRED")
                    continue

                if series_id == "ECB_API":
                    data = self.download_ecb_forex_reserves()
                    results[series_id] = data
                    if data is not None:
                        total_downloaded += 1
                    continue

                data = self.download_fred_series(series_id, series_name)
                results[series_id] = data
                
                if data is not None:
                    total_downloaded += 1
                
                time.sleep(0.5)  # Avoid request throttling
        
        print("\n" + "=" * 70)
        print("Download Completed!")
        print("=" * 70)
        print(f"\nSuccessfully downloaded: {total_downloaded}/{total_attempted} data series")
        print(f"Data saved to directory: {os.path.abspath(self.data_dir)}")
        
        return results
    
    def create_summary_report(self):
        """创建数据摘要报告"""
        csv_files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print("No downloaded data files found")
            return
        
        report_file = f"{self.data_dir}/Central_Bank_Data_Summary_Report.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("Central Bank Data Summary Report - Top 4 Economies\n")
            f.write("=" * 70 + "\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Number of data files: {len(csv_files)}\n")
            f.write(f"Time Range: January 1, 2020 to December 31, 2024\n\n")
            
            # Group by country/region
            country_groups = {
                'US': [],
                'Eurozone': [],
                'UK':[],
                'Japan': [],
                'China': [],
                'Global_Benchmark': []
            }
            
            for csv_file in sorted(csv_files):
                if "US_" in csv_file and "Global_Benchmark" not in csv_file:
                    country_groups['US'].append(csv_file)
                elif "Eurozone_" in csv_file:
                    country_groups['Eurozone'].append(csv_file)
                elif "UK_" in csv_file:
                    country_groups['UK'].append(csv_file)
                elif "Japan_" in csv_file:
                    country_groups['Japan'].append(csv_file)
                elif "China_" in csv_file:
                    country_groups['China'].append(csv_file)
                elif "Global_Benchmark" in csv_file:
                    country_groups['Global_Benchmark'].append(csv_file)
            
            for country, files in country_groups.items():
                if files:
                    f.write(f"\n{'=' * 70}\n")
                    f.write(f"【{country}】\n")
                    f.write(f"{'=' * 70}\n\n")
                    
                    for csv_file in files:
                        try:
                            df = pd.read_csv(f"{self.data_dir}/{csv_file}")
                            df['date'] = pd.to_datetime(df['date'])
                            
                            f.write(f"File: {csv_file}\n")
                            f.write(f"  Record count: {len(df)}\n")
                            f.write(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}\n")
                            
                            valid_values = df['value'].dropna()
                            if len(valid_values) > 0:
                                f.write(f"  Minimum value: {valid_values.min():.4f}\n")
                                f.write(f"  Maximum value: {valid_values.max():.4f}\n")
                                f.write(f"  Average value: {valid_values.mean():.4f}\n")
                                f.write(f"  Latest value: {valid_values.iloc[-1]:.4f}\n")
                            
                            f.write("\n")
                        except Exception as e:
                            f.write(f"File: {csv_file}\n")
                            f.write(f"  Error reading file: {e}\n\n")
        
        print(f"\nCentral bank data summary report generated: {report_file}")
    
    def create_comparison_table(self):
        """创建中国，美国，日本，英国央行政策利率对比表"""
        print("\n" + "=" * 70)
        print("Central Bank Policy Rate Comparison - Top 4 Economies (Latest Data)")
        print("=" * 70)
        
        comparison_data = []
        
        # Define key rates for comparison
        key_rates = {
            "US_Fed_Funds_Rate": "US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv",
            "UK_Deposit_Rate": "UK_Deposit_Interest_Rate_Monthly_IR3TIB01GBM156N.csv",
            "Japan_Short_Term_Rate": "Japan_Short_Term_Interest_Rate_Monthly_IRSTCI01JPM156N.csv",
            "China_Deposit_Rate": "China_Deposit_Interest_Rate_Monthly_INTDSRCNM193N.csv",
        }
        
        for rate_name, filename in key_rates.items():
            filepath = f"{self.data_dir}/{filename}"
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.dropna(subset=['value'])
                    
                    if not df.empty:
                        latest = df.iloc[-1]
                        one_year_ago = latest['date'] - timedelta(days=365)
                        historical = df[df['date'] <= one_year_ago]
                        
                        change_str = f"{latest['value'] - historical.iloc[-1]['value']:+.2f}" if not historical.empty else "N/A"
                        
                        # Get country/region
                        region = "US" if "US_" in rate_name else "Eurozone" if "Eurozone_" in rate_name else "Japan" if "Japan_" in rate_name else "China"
                        
                        comparison_data.append({
                            'Country/Region': region,
                            'Interest_Rate_Type': rate_name,
                            'Current_Rate(%)': f"{latest['value']:.2f}",
                            'Update_Date': latest['date'].date(),
                            '1-Year_Change(%)': change_str
                        })
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
        
        if comparison_data:
            comparison_df = pd.DataFrame(comparison_data)
            print("\n" + comparison_df.to_string(index=False))
            
            print("\nNote: Interest rates are in percentage (%)")
            comparison_file = f"{self.data_dir}/Central_Bank_Rate_Comparison.csv"
            comparison_df.to_csv(comparison_file, index=False, encoding='utf-8-sig')
            print(f"\nComparison table saved to: {comparison_file}")
        else:
            print("\nInsufficient data to generate comparison table")
    
    def create_forex_reserves_comparison(self):
        """创建外汇储备对比表"""
        print("\n" + "=" * 70)
        print("Foreign Exchange Reserves Comparison - Top 4 Economies (Latest Data)")
        print("=" * 70)
        
        reserves_data = []
        
        # 定义外汇储备文件名
        reserves_files = {
            "US": "US_Foreign_Exchange_Reserves_Monthly_TRESEGUSM052N.csv",
            "UK": "UK_Foreign_Exchange_Reserves_Monthly_TRESEGGBM052N.csv",
            "Japan": "Japan_Foreign_Exchange_Reserves_Monthly_INTDSRJP01JPM193N.csv",
            "China": "China_Foreign_Exchange_Reserves_Monthly_TRESEGCNM052N.csv",
        }
        
        for country, filename in reserves_files.items():
            filepath = f"{self.data_dir}/{filename}"
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.dropna(subset=['value'])
                    
                    if not df.empty:
                        latest = df.iloc[-1]
                        value_billion = latest['value'] / 1000  # Convert to billions USD
                        one_year_ago = latest['date'] - timedelta(days=365)
                        historical = df[df['date'] <= one_year_ago]
                        
                        if not historical.empty:
                            old_value = historical.iloc[-1]['value'] / 1000
                            change = value_billion - old_value
                            change_str = f"{change:+.0f}B ({(change/old_value)*100:+.1f}%)"
                        else:
                            change_str = "N/A"
                        
                        reserves_data.append({
                            'Country': country,
                            'Forex_Reserves(Billions USD)': f"{value_billion:.0f}",
                            'Update_Date': latest['date'].date(),
                            '1-Year_Change': change_str
                        })
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
        
        if reserves_data:
            reserves_df = pd.DataFrame(reserves_data)
            print("\n" + reserves_df.to_string(index=False))
            print("\nNote: Unit is Billions USD")
            
            
            reserves_file = f"{self.data_dir}/Forex_Reserves_Comparison.csv"
            reserves_df.to_csv(reserves_file, index=False, encoding='utf-8-sig')
            print(f"Reserves comparison table saved to: {reserves_file}")
        else:
            print("\nInsufficient data to generate reserves comparison table")

    


def main():
    """主函数"""
    print("=" * 70)
    print("Central Bank Data Download Tool")
    print("China | US | Japan | Eurozone")
    print("=" * 70)
    print("\nData Content:")
    print("  ✓ Central Bank Policy Rates (Benchmark Rates)")
    print("  ✓ Foreign Exchange Reserves")
    print("  ✓ Time Range: Jan 1, 2020 - Dec 31, 2024")
    print("\nData Source: FRED (Federal Reserve Economic Data)")
    print("=" * 70)
    
    downloader = CentralBankDataDownloader()
    downloader.download_all_central_bank_data()
    downloader.create_summary_report()
    downloader.create_comparison_table()
    downloader.create_forex_reserves_comparison()
    # download_additional_china_data()
    
    print("\n" + "=" * 70)
    print("All Data Download Tasks Completed!")
    print("=" * 70)
    print(f"\nData Storage Path: {os.path.abspath(downloader.data_dir)}/")
    print("\nGenerated Files:")
    print("  ✓ Central bank rates & forex reserves data (CSV format)")
    print("  ✓ Central_Bank_Data_Summary_Report.txt")
    print("  ✓ Central_Bank_Rate_Comparison.csv")
    print("  ✓ Forex_Reserves_Comparison.csv")
    
    print("\nUsage Suggestions:")
    print("  1. Use pandas to read CSV files for further analysis")
    print("  2. Combine with exchange rate data to study interest rate parity")
    print("  3. Analyze reserves changes vs exchange rate fluctuations")
    print("  4. Research monetary policy coordination across countries")
    
    print("\nNotes:")
    print("  ⚠️  China's central bank data is limited on FRED")
    print("  ⚠️  Recommend PBC official website for comprehensive China data")
    print("  ⚠️  Some data may have 1-2 month release delay")


if __name__ == "__main__":
    main()
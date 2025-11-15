import pandas as pd
import requests
import os
import time
from io import StringIO
from datetime import datetime

class MacroEconomicDataDownloader:
    """宏观经济数据下载器（仅保留原始下载逻辑，保持分类和命名不变）"""
    
    def __init__(self):
        self.data_dir = "macro_economic_data"  # 与原代码目录一致
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            print(f"Created data directory: {self.data_dir}")
        else:
            print(f"Using existing directory: {self.data_dir}")

    def download_fred_series(self, series_id, series_name):
        """从FRED下载数据，保持原命名规则：{series_name}_{series_id}.csv"""
        print(f"\nDownloading: {series_name} ({series_id})...")
        
        try:
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            df = pd.read_csv(StringIO(response.text))
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # 原代码的日期范围筛选（2020-2024）
            start_date = pd.to_datetime("2020-01-01")
            end_date = pd.to_datetime("2024-12-31")
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            
            if not df.empty:
                filename = f"{self.data_dir}/{series_name}_{series_id}.csv"  # 保持原文件名格式
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"✓ Successfully downloaded {len(df)} records")
                print(f"  Saved to: {filename}")
                print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
                
                latest = df.dropna(subset=['value']).tail(1)
                if not latest.empty:
                    print(f"  Latest value: {latest.iloc[0]['value']:.2f} ({latest.iloc[0]['date'].date()})")
                
                return df
            return None
            
        except Exception as e:
            print(f"✗ Download failed: {e}")
            return None

    def download_all_macro_data(self):
        """下载所有宏观经济数据，完全保持您原有的分类和序列命名"""
        print("=" * 70)
        print("Macro Economic Data Download Tool - Top 4 Economies")
        print("=" * 70)
        print("\nData Source: FRED (Federal Reserve Economic Data)")
        print("\nStarting download...\n")
        
        # 完全复制您原代码中的data_series结构，确保分类、ID、命名无任何改动
        data_series = [
            ("GDP and Economic Growth", [
                ("GDPC1", "US_Real_GDP_Quarterly_Billions_USD"),
                ("A191RL1Q225SBEA", "US_Real_GDP_Growth_Rate_Quarterly_Annualized"),

                ("NAEXKP01GBQ657S", "UK_Real_GDP_Growth_Rate_Quarterly_Annualized"), 
                ("UKNGDP", "UK_Real_GDP_Quarterly_Millions_GBP"), 

                ("JPNRGDPEXP", "Japan_Real_GDP_Quarterly"),
                ("JPNGDPRQPSMEI", "Japan_Real_GDP_Growth_Rate_Quarterly_Annualized"),

                ('NGDPRXDCCNA', "China_Real_GDP_Annually"),
                ("CHNGDPRAPSMEI", "China_Real_GDP_Growth_Rate_Annually_Annualized"),
            ]),
            ("Inflation_CPI", [
                ("CPIAUCSL", "US_CPI_Monthly_All_Urban_Consumers"),
                ("CPALTT01USM657N", "US_CPI_YoY_Growth_Rate_Monthly"),

                ("CPALTT01GBM657N", "UK_CPI_YoY_Growth_Rate_Monthly"),  
                ("GBRCPALTT01IXNBM", "UK_CPI_Monthly_All_Items"), 

                ("CPALTT01JPM659N", "Japan_CPI_YoY_Growth_Rate_Monthly"),
                ("JPNCPIALLMINMEI", "Japan_CPI_Monthly_All_Items"),

                ("CPALTT01CNM659N", "China_CPI_YoY_Growth_Rate_Monthly"),
                ("CHNCPIALLMINMEI", "China_CPI_Monthly_All_Items"),
            ]),
            ("Trade Balance", [
                ("XTEXVA01USQ188S", "US_Exports_Quarterly"),
                ("XTIMVA01USQ188S", "US_Imports_Quarterly"),

                ("XTEXVA01GBQ188S", "UK_Exports_Quarterly"),  
                ("XTIMVA01GBQ188S", "UK_Imports_Quarterly"),  

                ("XTEXVA01JPQ188S", "Japan_Exports_Quarterly"),
                ("XTIMVA01JPQ188S", "Japan_Imports_Quarterly"),

                ("CHNXTEXVA01STSAQ", "China_Exports_Quarterly"),
                ("CHNXTIMVA01STSAQ", "China_Imports_Quarterly"),
            ]),
            ("Unemployment Rate", [
                ("UNRATE", "US_Unemployment_Rate_Monthly"),

                ("LRHUTTTTGBM156S", "UK_Unemployment_Rate_Monthly"),

                ("LRHUTTTTJPM156S", "Japan_Unemployment_Rate_Monthly"),

                ("SLUEM1524ZSCHN", "China_Youth_Unemployment_Rate_Annually"),
            ]),
            ("Industrial Production", [
                ("INDPRO", "US_Industrial_Production_Index_Monthly"),

                ("JPNPRINTO01IXOBM", "Japan_Industrial_Production_Index_Monthly"),

                ("PRCNTO01CNQ661S", "China_Industrial_Production_Index_Quarterly"),
            ]),
            ("Consumer Confidence", [
                ("CSCICP03USM665S", "US_Consumer_Confidence_Index_Monthly"),

                ("CSCICP03GBM665S", "UK_Consumer_Confidence_Index_Monthly"),  

                ("CSCICP03JPM665S", "Japan_Consumer_Confidence_Index_Monthly"),

                ("CSCICP03CNM665S", "China_Consumer_Confidence_Index_Monthly"),
            ]),
            ("Government Debt", [
                ("DEBTTLUSA188A", "US_Government_Debt_to_GDP_Ratio_Annually"),

                ("DEBTTLGBA188A", "UK_Government_Debt_to_GDP_Ratio_Annually"), 

                ("DEBTTLJPA188A", "Japan_Government_Debt_to_GDP_Ratio_Annually"),
            ]),
            ("Retail Sales", [
                ("SLRTTO02USA189S", "US_Retail_Sales_value_Monthly"),
                ("USASLRTTO01GYSAM", "US_Retail_Sales_volume_Monthly"),

                ("GBRSLRTTO02IXOBSAM", "UK_Retail_Sales_value_Monthly"),  
                ("GBRSLRTTO01GPSAM", "UK_Retail_Sales_volume_Monthly"),

                ("JPNSLRTTO02IXOBSAM", "Japan_Retail_Sales_value_Monthly"),
                ("JPNSLRTTO01GPSAM", "Japan_Retail_Sales_volume_Monthly"),
            ]),
            ("House Price Index", [
                ("QUSR628BIS", "US_House_Price_Index_Quarterly"),

                ("QGBR628BIS", "UK_House_Price_Index_Quarterly"), 

                ("QJPR368BIS", "Japan_House_Price_Index_Quarterly"),

                ("QCNR628BIS", "China_House_Price_Index_Quarterly"),
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
                data = self.download_fred_series(series_id, series_name)
                results[series_id] = data
                
                if data is not None:
                    total_downloaded += 1
                
                time.sleep(0.5)  # 保持原延迟，避免请求限流
        
        print("\n" + "=" * 70)
        print("Download Completed!")
        print("=" * 70)
        print(f"\nSuccessfully downloaded: {total_downloaded}/{total_attempted} data series")
        print(f"Data saved to directory: {os.path.abspath(self.data_dir)}")
        
        return results


def main():
    """主函数：仅执行下载，后续处理/可视化在其他文件中进行"""
    print("=" * 70)
    print("Macro Economic Data Download Tool - Top 4 Economies")
    print("US | UK | Japan | China")
    print("=" * 70)
    print("\nOnly download raw data. Data processing and visualization will be in separate files.")
    print("=" * 70)
    
    downloader = MacroEconomicDataDownloader()
    downloader.download_all_macro_data()
    
    print("\n" + "=" * 70)
    print("All Download Tasks Completed!")
    print("=" * 70)
    print(f"\nData Storage Path: {os.path.abspath(downloader.data_dir)}/")
    print("\nProceed to your data processing and visualization scripts.")


if __name__ == "__main__":
    main()
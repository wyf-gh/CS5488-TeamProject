import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib


matplotlib.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

class MacroEconomicVisualizer:
    def __init__(self, data_dir="macro_economic_data", image_dir="Macro_Economic_Images"):
        self.data_dir = data_dir
        self.image_dir = image_dir
        if not os.path.exists(image_dir):
            os.makedirs(image_dir)
        
        # 指标-文件映射（严格匹配用户提供的实际文件名）
        self.indicator_files = {
            # GDP 增长 
            "GDP Growth": {
                "US": "US_Real_GDP_Growth_Rate_Quarterly_Annualized_A191RL1Q225SBEA.csv",
                "UK": "UK_Real_GDP_Growth_Rate_Quarterly_Annualized_NAEXKP01GBQ657S.csv",
                "Japan": "Japan_Real_GDP_Growth_Rate_Quarterly_Annualized_JPNGDPRQPSMEI.csv",
                "China": "China_Real_GDP_Growth_Rate_Annually_Annualized_CHNGDPRAPSMEI.csv"
            },
            
            # CPI 月度 + YoY + 核心CPI（仅美国）
            "CPI Monthly": {
                "US": "US_CPI_Monthly_All_Urban_Consumers_CPIAUCSL.csv",
                "UK": "UK_CPI_Monthly_All_Items_GBRCPALTT01IXNBM.csv",
                "Japan": "Japan_CPI_Monthly_All_Items_JPNCPIALLMINMEI.csv",
                "China": "China_CPI_Monthly_All_Items_CHNCPIALLMINMEI.csv"
            },
            "CPI YoY": {
                "US": "US_CPI_YoY_Growth_Rate_Monthly_CPALTT01USM657N.csv",
                "UK": "UK_CPI_YoY_Growth_Rate_Monthly_CPALTT01GBM657N.csv",
                "Japan": "Japan_CPI_YoY_Growth_Rate_Monthly_CPALTT01JPM659N.csv",
                "China": "China_CPI_YoY_Growth_Rate_Monthly_CPALTT01CNM659N.csv"
            },
            "Core CPI": {
                "US": "US_Core_CPI_Monthly_Excluding_Food_Energy_CPILFESL.csv"
            },
            # 进出口
            "Exports": {
                "US": "US_Exports_Quarterly_XTEXVA01USQ188S.csv",
                "UK": "UK_Exports_Quarterly_XTEXVA01GBQ188S.csv",
                "Japan": "Japan_Exports_Quarterly_XTEXVA01JPQ188S.csv",
                "China": "China_Exports_Quarterly_CHNXTEXVA01STSAQ.csv"
            },
            "Imports": {
                "US": "US_Imports_Quarterly_XTIMVA01USQ188S.csv",
                "UK": "UK_Imports_Quarterly_XTIMVA01GBQ188S.csv",
                "Japan": "Japan_Imports_Quarterly_XTIMVA01JPQ188S.csv",
                "China": "China_Imports_Quarterly_CHNXTIMVA01STSAQ.csv"
            },
            # 零售额（价值+数量）
            "Retail Sales": {
                "US_Value": "US_Retail_Sales_value_Monthly_SLRTTO02USA189S.csv",
                "US_Volume": "US_Retail_Sales_volume_Monthly_USASLRTTO01GYSAM.csv",
                "UK_Value": "UK_Retail_Sales_value_Monthly_GBRSLRTTO02IXOBSAM.csv",
                "UK_Volume": "UK_Retail_Sales_volume_Monthly_GBRSLRTTO01GPSAM.csv",
                "Japan_Value": "Japan_Retail_Sales_value_Monthly_JPNSLRTTO02IXOBSAM.csv",
                "Japan_Volume": "Japan_Retail_Sales_volume_Monthly_JPNSLRTTO01GPSAM.csv"
            },
            # 工业生产指数
            "Industrial Production Index": {
                "US": "US_Industrial_Production_Index_Monthly_INDPRO.csv",
                "UK": "UK_Industrial_Production_Index_Monthly_IPIUKM.csv",
                "Japan": "Japan_Industrial_Production_Index_Monthly_JPNPRINTO01IXOBM.csv",
                "China": "China_Industrial_Production_Index_Quarterly_PRC NTO01CNQ661S.csv"
            },
            # 失业率
            "Unemployment Rate": {
                "US": "US_Unemployment_Rate_Monthly_UNRATE.csv",
                "UK": "UK_Unemployment_Rate_Monthly_LRHUTTTTGBM156S.csv",
                "Japan": "Japan_Unemployment_Rate_Monthly_LRHUTTTTJPM156S.csv",
                "China": "China_Youth_Unemployment_Rate_Annually_SLUEM1524ZSCHN.csv"
            },
            # 消费者信心指数
            "Consumer Confidence": {
                "US": "US_Consumer_Confidence_Index_Monthly_CSCICP03USM665S.csv",
                "UK": "UK_Consumer_Confidence_Index_Monthly_CSCICP03GBM665S.csv",
                "Japan": "Japan_Consumer_Confidence_Index_Monthly_CSCICP03JPM665S.csv",
                "China": "China_Consumer_Confidence_Index_Monthly_CSCICP03CNM665S.csv"
            },
            # 房价指数
            "House Price Index": {
                "US": "US_House_Price_Index_Quarterly_QUSR628BIS.csv",
                "UK": "UK_House_Price_Index_Quarterly_QGBR628BIS.csv",
                "Japan": "Japan_House_Price_Index_Quarterly_QJPR368BIS.csv",
                "China": "China_House_Price_Index_Quarterly_QCNR628BIS.csv"
            },
            # 政府债务/GDP比率
            "Government Debt to GDP": {
                "US": "US_Government_Debt_to_GDP_Ratio_Annually_DEBTTLUSA188A.csv",
                "UK": "UK_Government_Debt_to_GDP_Ratio_Annually_DEBTTLGBA188A.csv",
                "Japan": "Japan_Government_Debt_to_GDP_Ratio_Annually_DEBTTLJPA188A.csv"
            }
        }

    def load_data(self, indicator, country=None):
        """加载数据并严格筛选2020-2024范围"""
        data_dict = {}
        if country:
            # 处理带空格的文件名（如中国工业生产指数）
            filename = self.indicator_files[indicator][country].replace(" ", "") if " " in self.indicator_files[indicator][country] else self.indicator_files[indicator][country]
            file_path = os.path.join(self.data_dir, filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['date'] = pd.to_datetime(df['date'])
                df = df[(df['date'] >= "2020-01-01") & (df['date'] <= "2024-12-31")]
                data_dict[country] = df
            else:
                print(f"✗ File not found: {file_path} → {country} {indicator} data will be skipped.")
        else:
            for ctry, filename in self.indicator_files[indicator].items():
                # 处理带空格的文件名
                filename = filename.replace(" ", "") if " " in filename else filename
                file_path = os.path.join(self.data_dir, filename)
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df[(df['date'] >= "2020-01-01") & (df['date'] <= "2024-12-31")]
                    data_dict[ctry] = df
        return data_dict

    def visualize_gdp(self):
        """可视化GDP增长（调整纵轴范围）和名义值"""
        # GDP增长
        growth_data = self.load_data("GDP Growth")
        plt.figure(figsize=(12, 6))
        for country, df in growth_data.items():
            if country == "China":
                # 中国年度数据，用散点和文本标注
                plt.scatter(df['date'], df['value'], label=f"{country} (Annual)", marker='o', s=100, color='red')
                for i, row in df.iterrows():
                    plt.text(row['date'], row['value'] + 0.5, f"{row['value']:.2f}%", ha='center')
            else:
                # 其他国家季度数据，折线图
                plt.plot(df['date'], df['value'], label=country, linewidth=2)
        plt.ylim(-40, 40)  # 调整纵轴范围，清晰展示各国增长率差异
        plt.title("GDP Growth Rate (2020-2024)")
        plt.xlabel("Date")
        plt.ylabel("Growth Rate (%)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(self.image_dir, "gdp_growth_visualization.png"))
        plt.close()

       

    def visualize_cpi(self):
        """CPI月度和CPI YoY+核心CPI 分开为两个子图"""
        # 月度CPI
        monthly_data = self.load_data("CPI Monthly")
        # YoY CPI和核心CPI
        yoy_data = self.load_data("CPI YoY")
        core_data = self.load_data("Core CPI")
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))
        # 子图1：月度CPI
        for country, df in monthly_data.items():
            ax1.plot(df['date'], df['value'], label=f"{country} CPI (Monthly)", linewidth=2)
        ax1.set_title("CPI Monthly Index (2020-2024)")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("CPI Index")
        ax1.legend()
        ax1.grid(True, linestyle='--', alpha=0.7)
        
        # 子图2：CPI YoY和核心CPI
        for country, df in yoy_data.items():
            ax2.plot(df['date'], df['value'], label=f"{country} CPI YoY", linewidth=2)
        if "US" in core_data:
            ax2.plot(core_data["US"]['date'], core_data["US"]['value'], label="US Core CPI", linewidth=2, color='green')
        ax2.set_title("CPI YoY Growth Rate and Core CPI (2020-2024)")
        ax2.set_xlabel("Date")
        ax2.set_ylabel("CPI YoY (%)")
        ax2.legend()
        ax2.grid(True, linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.image_dir, "cpi_visualization.png"))
        plt.close()
        print("✓ CPI 两个子图可视化完成。")

    def visualize_exports_imports(self):
        """进出口合并子图"""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))
        # 出口
        exports_data = self.load_data("Exports")
        for country, df in exports_data.items():
            ax1.plot(df['date'], df['value'], label=country, linewidth=2, marker='o', markersize=4)
        ax1.set_title("Exports Value (2020-2024)")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Exports Value (Currency Unit)")
        ax1.legend()
        ax1.grid(True, linestyle='--', alpha=0.7)
        # 进口
        imports_data = self.load_data("Imports")
        for country, df in imports_data.items():
            ax2.plot(df['date'], df['value'], label=country, linewidth=2, marker='o', markersize=4)
        ax2.set_title("Imports Value (2020-2024)")
        ax2.set_xlabel("Date")
        ax2.set_ylabel("Imports Value (Currency Unit)")
        ax2.legend()
        ax2.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(os.path.join(self.image_dir, "exports_imports_combined.png"))
        plt.close()
        print("✓ Exports and imports combined subplots visualization completed.")

    def visualize_retail_sales(self):
        """零售额：仅保留三国子图（价值+数量双Y轴）"""
        countries = ["US", "UK", "Japan"]
        fig, axes = plt.subplots(3, 1, figsize=(12, 18))
        for i, country in enumerate(countries):
            value_df = self.load_data("Retail Sales", f"{country}_Value")[f"{country}_Value"]
            volume_df = self.load_data("Retail Sales", f"{country}_Volume")[f"{country}_Volume"]
            merged = pd.merge(value_df, volume_df, on='date', suffixes=('_Value', '_Volume'))
            
            ax = axes[i]
            ax1 = ax
            ax2 = ax.twinx()
            ax1.plot(merged['date'], merged['value_Value'], label="Retail Sales (Value)", color='blue', linewidth=2)
            ax2.plot(merged['date'], merged['value_Volume'], label="Retail Sales (Volume)", color='orange', linewidth=2, linestyle='--')
            ax1.set_title(f"{country} Retail Sales (Value vs Volume)")
            ax1.set_xlabel("Date")
            ax1.set_ylabel("Retail Sales (Value)", color='blue')
            ax2.set_ylabel("Retail Sales (Volume)", color='orange')
            ax1.legend(loc='upper left')
            ax2.legend(loc='upper right')
            ax.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(os.path.join(self.image_dir, "retail_sales_country_subplots.png"))
        plt.close()
        print("✓ 零售额三国子图可视化完成。")

    def visualize_industrial_production(self):
        """可视化工业生产指数"""
        ip_data = self.load_data("Industrial Production Index")
        plt.figure(figsize=(12, 8))
        for country, df in ip_data.items():
            plt.plot(df['date'], df['value'], label=country, linewidth=2)
        plt.title("Industrial Production Index (2020-2024)")
        plt.xlabel("Date")
        plt.ylabel("Index (2012=100)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(self.image_dir, "industrial_production_visualization.png"))
        plt.close()
        print("✓ Industrial Production Index visualization completed.")

    def visualize_unemployment(self):
        """可视化失业率（中国标注青年、年度）"""
        unemp_data = self.load_data("Unemployment Rate")
        plt.figure(figsize=(12, 8))
        for country, df in unemp_data.items():
            if country == "China":
                plt.plot(df['date'], df['value'], label=f"{country} (Youth, Annual)", linewidth=2, linestyle='--', color='red')
                plt.text(df['date'].iloc[-1], df['value'].iloc[-1] + 0.2, "青年失业率（年度）", ha='right')
            else:
                plt.plot(df['date'], df['value'], label=country, linewidth=2)
        plt.title("Unemployment Rate (2020-2024)")
        plt.xlabel("Date")
        plt.ylabel("Unemployment Rate (%)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(self.image_dir, "unemployment_visualization.png"))
        plt.close()
        print("✓ Unemployment Rate visualization completed.")

    def visualize_consumer_confidence(self):
        """可视化消费者信心指数"""
        conf_data = self.load_data("Consumer Confidence")
        plt.figure(figsize=(12, 8))
        for country, df in conf_data.items():
            plt.plot(df['date'], df['value'], label=country, linewidth=2)
        plt.title("Consumer Confidence Index (2020-2024)")
        plt.xlabel("Date")
        plt.ylabel("Index (50 = Neutral)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(self.image_dir, "consumer_confidence_visualization.png"))
        plt.close()
        print("✓ Consumer Confidence Index visualization completed.")

    def visualize_house_price(self):
        """可视化房价指数"""
        hp_data = self.load_data("House Price Index")
        plt.figure(figsize=(12, 8))
        for country, df in hp_data.items():
            plt.plot(df['date'], df['value'], label=country, linewidth=2)
        plt.title("House Price Index (2020-2024)")
        plt.xlabel("Date")
        plt.ylabel("Index (2015=100)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(self.image_dir, "house_price_visualization.png"))
        plt.close()
        print("✓ House Price Index visualization completed.")

    def visualize_government_debt(self):
        """可视化政府债务/GDP比率"""
        debt_data = self.load_data("Government Debt to GDP")
        plt.figure(figsize=(12, 8))
        for country, df in debt_data.items():
            plt.plot(df['date'], df['value'], label=country, linewidth=2)
        plt.title("Government Debt to GDP Ratio (2020-2024)")
        plt.xlabel("Date")
        plt.ylabel("Debt/GDP Ratio (%)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(self.image_dir, "government_debt_visualization.png"))
        plt.close()
        print("✓ Government Debt to GDP Ratio visualization completed.")

    def visualize_all(self):
        """生成所有指标的可视化"""
        self.visualize_gdp()
        self.visualize_cpi()
        self.visualize_exports_imports()
        self.visualize_retail_sales()
        self.visualize_industrial_production()
        self.visualize_unemployment()
        self.visualize_consumer_confidence()
        self.visualize_house_price()
        self.visualize_government_debt()
        print("✓ All visualization tasks completed!")

def main():
    print("=" * 70)
    print("Macro Economic Data Visualization Tool - Revised Version")
    print("Covers: GDP, CPI (Monthly + YoY), Exports-Imports, Retail Sales (Subplots Only), etc.")
    print("=" * 70)
    
    visualizer = MacroEconomicVisualizer(
        data_dir="macro_economic_data", 
        image_dir="Marco_Economic_Images"
    )
    visualizer.visualize_all()
    
    print("\n" + "=" * 70)
    print("All Visualization Tasks Completed!")
    print("=" * 70)
    print(f"Visualization files saved to: {os.path.abspath(visualizer.image_dir)}")
    print("\nVisualization Explanation:")
    print("- GDP: Growth rate (adjusted y-axis for clarity) and nominal value plots.")
    print("- CPI: Two subplots for Monthly Index and YoY Growth Rate + US Core CPI.")
    print("- Exports-Imports: Combined subplots with line + markers.")
    print("- Retail Sales: Only country-specific subplots (dual Y-axis for value/volume).")
    print("- Industrial Production, Unemployment, Consumer Confidence, House Price, Government Debt: Each has a separate visualization.")

if __name__ == "__main__":
    main()
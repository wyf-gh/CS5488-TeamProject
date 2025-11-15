USE bigdata;

LOAD DATA INPATH '/user/master/exchange_rate_data/GBPUSD.csv' INTO TABLE exchange_rate;
LOAD DATA INPATH '/user/master/exchange_rate_data/USDCNY.csv' INTO TABLE exchange_rate;
LOAD DATA INPATH '/user/master/exchange_rate_data/USDJPY.csv' INTO TABLE exchange_rate;


LOAD DATA INPATH '/user/master/world_bank_wdi_data/REER_index.csv' INTO TABLE world_bank;
LOAD DATA INPATH '/user/master/world_bank_wdi_data/PPP_conversion.csv' INTO TABLE world_bank;
LOAD DATA INPATH '/user/master/world_bank_wdi_data/GDP_growth.csv' INTO TABLE world_bank;
LOAD DATA INPATH '/user/master/world_bank_wdi_data/Inflation.csv' INTO TABLE world_bank;
LOAD DATA INPATH '/user/master/world_bank_wdi_data/Current_account.csv' INTO TABLE world_bank;


LOAD DATA INPATH '/user/master/central_bank_data/US_Effective_Federal_Funds_Rate_Daily_EFFR.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/UK_Foreign_Exchange_Reserves_Monthly_TRESEGGBM052N.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/Japan_Foreign_Exchange_Reserves_Monthly_TRESEGJPM052N.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/Eurozone_Foreign_Exchange_Reserves_Monthly.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/China_Foreign_Exchange_Reserves_Monthly_TRESEGCNM052N.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/US_Secured_Overnight_Financing_Rate_Daily_SOFR.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/US_Interest_Rate_on_Reserves_Daily_IORB.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/US_Fed_Funds_Target_Rate_Monthly_FEDFUNDS.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/UK_Deposit_Interest_Rate_Monthly_IR3TIB01GBM156N.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/Japan_Short-Term_Interest_Rate_Monthly_IRSTCI01JPM156N.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/Eurozone_ECB_Main_Refinancing_Rate_Daily_ECBMRRFR.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/Eurozone_ECB_Deposit_Facility_Rate_Daily_ECBDFR.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/China_Deposit_Interest_Rate_Monthly_INTDSRCNM193N.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/US_Fed_Funds_Effective_Rate_Daily_DFF.csv' INTO TABLE central_bank;
LOAD DATA INPATH '/user/master/central_bank_data/US_Foreign_Exchange_Reserves_Monthly_TRESEGUSM052N.csv' INTO TABLE central_bank;


LOAD DATA INPATH '/user/master/macro_economic_data/China_Consumer_Confidence_Index_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_CPI_Monthly_All_Items.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_CPI_YoY_Growth_Rate_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_Exports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_House_Price_Index_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_Imports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_Industrial_Production_Index_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_Real_GDP_Annually.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_Real_GDP_Growth_Rate_Annually_Annualized.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/China_Youth_Unemployment_Rate_Annually.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Consumer_Confidence_Index_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_CPI_Monthly_All_Items.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_CPI_YoY_Growth_Rate_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Exports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Government_Debt_to_GDP_Ratio_Annually.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_House_Price_Index_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Imports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Industrial_Production_Index_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Real_GDP_Growth_Rate_Quarterly_Annualized.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Real_GDP_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Retail_Sales_value_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Retail_Sales_volume_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/Japan_Unemployment_Rate_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Consumer_Confidence_Index_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_CPI_Monthly_All_Items.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_CPI_YoY_Growth_Rate_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Exports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Government_Debt_to_GDP_Ratio_Annually.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_House_Price_Index_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Imports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Real_GDP_Growth_Rate_Quarterly_Annualized.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Real_GDP_Quarterly_Millions_GBP.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Retail_Sales_value_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Retail_Sales_volume_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/UK_Unemployment_Rate_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Consumer_Confidence_Index_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_CPI_Monthly_All_Urban_Consumers.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_CPI_YoY_Growth_Rate_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Exports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Government_Debt_to_GDP_Ratio_Annually.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_House_Price_Index_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Imports_Quarterly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Industrial_Production_Index_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Real_GDP_Growth_Rate_Quarterly_Annualized.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Real_GDP_Quarterly_Billions_USD.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Retail_Sales_value_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Retail_Sales_volume_Monthly.csv' INTO TABLE macro_economic;
LOAD DATA INPATH '/user/master/macro_economic_data/US_Unemployment_Rate_Monthly.csv' INTO TABLE macro_economic;
CREATE DATABASE bigdata;
USE bigdata;

CREATE TABLE IF NOT EXISTS exchange_rate (
    dateinfo STRING COMMENT 'Date in yyyy/MM/dd format',
    tricker STRING COMMENT 'Trading pair identifier, e.g. GBPUSD=X',
    open_price DOUBLE COMMENT 'Opening price',
    high_price DOUBLE COMMENT 'Highest price',
    low_price DOUBLE COMMENT 'Lowest price',
    close_price DOUBLE COMMENT 'Closing price'
)
COMMENT 'Historical GBP/USD exchange rate data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1' )
;


CREATE TABLE IF NOT EXISTS world_bank (
    series_name STRING COMMENT 'Indicator name ',
    series_code STRING COMMENT 'Indicator code ',
    country_name STRING COMMENT 'Country name ',
    country_code STRING COMMENT 'Country code ',
    yr2020 DOUBLE COMMENT 'Data for 2020',
    yr2021 DOUBLE COMMENT 'Data for 2021',
    yr2022 DOUBLE COMMENT 'Data for 2022',
    yr2023 DOUBLE COMMENT 'Data for 2023',
    yr2024 DOUBLE COMMENT 'Data for 2024'
)
COMMENT 'Real Effective Exchange Rate Index Data by Country (2020-2024)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1' )
;

CREATE TABLE IF NOT EXISTS central_bank (
    `Region` STRING,
    `Data_Type` STRING,
    `Interest_Rate_Type` STRING,
    `Frequency` STRING,
    `Unit` STRING,
    `dateinfo` STRING,
    `datavalue` DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1')
;


CREATE TABLE IF NOT EXISTS macro_economic (
    `Region` STRING,
    `Indicator_Type` STRING,
    `Indicator_Name` STRING,
    `Frequency` STRING,
    `dateinfo` STRING,
    `datavalue` DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1')
;

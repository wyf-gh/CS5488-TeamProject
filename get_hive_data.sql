
USE bigdata;

-- 1. 导出 central_bank_new
INSERT OVERWRITE DIRECTORY '/user/hive/export/bigdata/central_bank_new' SELECT * FROM central_bank_new;

-- 2. 导出 central_bank_quarterly
INSERT OVERWRITE DIRECTORY '/user/hive/export/bigdata/central_bank_quarterly' SELECT * FROM central_bank_quarterly;

-- 3. 导出 exchange_rate_new
INSERT OVERWRITE DIRECTORY '/user/hive/export/bigdata/exchange_rate_new' SELECT * FROM exchange_rate_new;

-- 4. 导出 exchange_rate_quarterly
INSERT OVERWRITE DIRECTORY '/user/hive/export/bigdata/exchange_rate_quarterly' SELECT * FROM exchange_rate_quarterly;

-- 5. 导出 macro_economic_new
INSERT OVERWRITE DIRECTORY '/user/hive/export/bigdata/macro_economic_new' SELECT * FROM macro_economic_new;

-- 6. 导出 macro_economic_quarterly
INSERT OVERWRITE DIRECTORY '/user/hive/export/bigdata/macro_economic_quarterly' SELECT * FROM macro_economic_quarterly;
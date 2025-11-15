macro_data = LOAD 'bigdata.macro_economic' USING org.apache.hive.hcatalog.pig.HCatLoader();


macro_processed_data = FILTER macro_data
    BY datavalue IS NOT NULL;

STORE macro_processed_data INTO '/user/master/cleaned_macro' USING PigStorage(',');

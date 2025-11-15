
raw_data= LOAD 'bigdata.exchange_rate' USING org.apache.hive.hcatalog.pig.HCatLoader();

clean_data = FILTER raw_data BY close_price > 0 AND high_price >= low_price;

data_with_return = FOREACH clean_data GENERATE dateinfo, tricker, open_price, close_price, (close_price - open_price)/open_price AS daily_return;

STORE data_with_return INTO '/user/master/cleaned_exchange' USING PigStorage(',');
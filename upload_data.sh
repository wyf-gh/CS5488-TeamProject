hadoop fs -mkdir -p /user/master/exchange_rate_data
hadoop fs -put /media/sf_big_data/exchange_rate_data/* /user/master/exchange_rate_data/
echo "Completely upload exchange rate data!"


hadoop fs -mkdir -p /user/master/world_bank_wdi_data
hadoop fs -put /media/sf_big_data/world_bank_wdi_data/* /user/master/world_bank_wdi_data/
echo "Completely upload world bank wdi data!"


hadoop fs -mkdir -p /user/master/central_bank_data
hadoop fs -put /media/sf_big_data/central_bank_data/* /user/master/central_bank_data/
echo "Completely upload central bank data!"


hadoop fs -mkdir -p /user/master/macro_economic_data
hadoop fs -put /media/sf_big_data/macro_economic_data/* /user/master/macro_economic_data/
echo "Completely upload macro economic data"


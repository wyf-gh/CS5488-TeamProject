!/bin/bash


hdfs dfs -get /user/hive/export/bigdata/central_bank_new /media/sf_big_data/hive_export/
hdfs dfs -get /user/hive/export/bigdata/central_bank_quarterly /media/sf_big_data/hive_export/
hdfs dfs -get /user/hive/export/bigdata/exchange_rate_new /media/sf_big_data/hive_export/
hdfs dfs -get /user/hive/export/bigdata/exchange_rate_quarterly /media/sf_big_data/hive_export/
hdfs dfs -get /user/hive/export/bigdata/macro_economic_new /media/sf_big_data/hive_export/
hdfs dfs -get /user/hive/export/bigdata/macro_economic_quarterly /media/sf_big_data/hive_export/


INPUT_DIR="/media/sf_big_data/hive_export"  

OUTPUT_DIR="/media/sf_big_data/hive_csv"

mkdir -p $OUTPUT_DIR


for table_dir in ./hive_export/*; do
  table_name=$(basename $table_dir)
  mkdir -p ./hive_csv/$table_name
  for file in $table_dir/*; do
 
    tr '\001' ',' < $file > ./hive_csv/$table_name/$(basename $file).csv
    echo "Transform to :./hive_csv/$table_name/$(basename $file).csv"
  done
done

echo "Stored in $OUTPUT_DIR"


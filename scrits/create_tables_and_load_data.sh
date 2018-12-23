SPARK_ENGINE=spark
TEZ_ENGINE=tez
MR_ENGINE=mr

CITY_TABLE=city
BIDS_TABLE=bids
BIDS_TABLE_PLAINTEXT_PARTITIONED=bids_m
BIDS_TABLE_ORC_PARTITIONED=bids_orc

echo "Creating tables..."

hive -d CITY_TABLE_NAME=${CITY_TABLE} \
	-d CITY_FILE_PATH=/tmp/bids/city/city.en.txt \
	-f create_table_city.hql

hive -d BIDS_TABLE_NAME=${BIDS_TABLE} \
	-d BIDS_DATA_LOCATION=/tmp/bids/data
	-f create_table_bids.hql

hive -d BIDS_TABLE_NAME=${BIDS_TABLE_PLAINTEXT_PARTITIONED} \
	-d BIDS_EXT_TABLE_NAME=${BIDS_TABLE}
	-f create_partitioned_table_bids.hql

hive -d BIDS_TABLE_NAME=${BIDS_TABLE_ORC_PARTITIONED} \
	-d BIDS_EXT_TABLE_NAME=${BIDS_TABLE}
	-f create_partitioned_table_bids_orc.hql

ENGINE=$1
BIDS_TABLE=$2

for run in {1..10}
do
  hive -d HIVE_EXEC_ENGINE=${ENGINE} \
  	-d VECTORIZATION=true \
  	-d UA_JAR_PATH=/root/udf.jar \
  	-d BIDS_TABLE_NAME=${BIDS_TABLE} \
  	-d CITY_TABLE_NAME=city \
  	-f select_top_ua_for_each_city.hql
done
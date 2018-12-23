DROP TABLE IF EXISTS ${BIDS_TABLE_NAME};

CREATE TABLE ${BIDS_TABLE_NAME} (
	bidID string,
	time TIMESTAMP,
	ipinyouid string,
	useragent string,
	ip string,
	regionId int,
	cityId int,
	adexchange int,
	domain string,
	url string,
	anonUrl string,
	adslotId string,
	adslotwidth int,
	adslotheight int,
	adslotvisibility string,
	adslotformat string,
	addslotfloorprice double,
	creativeid string,
	biddingprice double,
	advertiserid int,
	userprofileids array<int>
)
PARTITIONED BY (date_col string)
CLUSTERED BY(cityId) INTO 334 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
	COLLECTION ITEMS TERMINATED BY ','
    LINES TERMINATED BY '\n';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;

INSERT OVERWRITE TABLE ${BIDS_TABLE_NAME} PARTITION (date_col) SELECT 
	bidID,
	ipinyouid,
	useragent,
	ip,
	regionId,
	cityId,
	adexchange,
	domain,
	url,
	anonUrl,
	adslotId,
	adslotwidth,
	adslotheight,
	adslotvisibility,
	adslotformat,
	addslotfloorprice,
	creativeid,
	biddingprice,
	advertiserid,
	userprofileids,
	to_date(time) as date_col,
FROM ${BIDS_EXT_TABLE_NAME};

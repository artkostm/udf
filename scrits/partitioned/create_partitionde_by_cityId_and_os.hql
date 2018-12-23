DROP TABLE IF EXISTS ${BIDS_TABLE_NAME};

CREATE TABLE ${BIDS_TABLE_NAME} (
	bidID string,
	time TIMESTAMP,
	ipinyouid string,
	useragent string,
	ip string,
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
PARTITIONED BY (cityId int, os string)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
	COLLECTION ITEMS TERMINATED BY ','
    LINES TERMINATED BY '\n';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

ADD JAR ${UA_JAR_PATH};

CREATE TEMPORARY FUNCTION ua AS 'by.artsiom.bigdata201.udf.UAGUDF';

INSERT OVERWRITE TABLE ${BIDS_TABLE_NAME} PARTITION (cityId, os) SELECT 
	bidID,
	time,
	ipinyouid,
	useragent,
	ip,
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
	regionId,
	cityId,
	ua(useragent).os as os
FROM ${BIDS_EXT_TABLE_NAME};

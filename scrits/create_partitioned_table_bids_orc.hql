DROP TABLE IF EXISTS ${BIDS_TABLE_NAME};

CREATE TABLE bids_orc (
	bidID string,
	time TIMESTAMP,
	ipinyouid string,
	useragent string,
	ip string,
	regionId int,
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
PARTITIONED BY (cityId int)
STORED AS ORC
TBLPROPERTIES('ORC.COMPRESS'='SNAPPY');

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${BIDS_TABLE_NAME} PARTITION (cityId) SELECT 
	bidID,
	time,
	ipinyouid,
	useragent,
	ip,
	regionId,
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
	cityId 
FROM ${BIDS_EXT_TABLE_NAME};

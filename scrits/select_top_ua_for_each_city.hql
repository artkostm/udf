SET spark.master=yarn-client;
SET hive.execution.engine=${HIVE_EXEC_ENGINE};

ADD JAR ${UA_JAR_PATH};

CREATE TEMPORARY FUNCTION ua AS 'by.artsiom.bigdata201.udf.UAGUDF';

SELECT /*+ MAPJOIN(c) */ DISTINCT 
	c.name, 
	b3.os, 
	b3.device, 
	b3.browser 
FROM (
	SELECT 
		b2.cityId, 
		b2.os, 
		b2.device, 
		b2.browser, 
		COUNT(*) as count, 
		RANK() OVER (PARTITION BY b2.cityId ORDER BY count(*) DESC) AS rnk 
	FROM (
		SELECT
			ua(b1.useragent).os,
			ua(b1.useragent).device,
			ua(b1.useragent).browser,
			b1.cityId
		FROM ${BIDS_TABLE_NAME} b1
	) b2
	GROUP BY b2.cityId, b2.os, b2.device, b2.browser
) b3
JOIN ${CITY_TABLE_NAME} c ON c.cityId = b3.cityId
WHERE b3.rnk = 1
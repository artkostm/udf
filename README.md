### Task 1

Build jar: ```./gradlew shadowJar```

To use the udf with HIVE just add the following script:

```sql
ADD JAR /path/to/udf.jar;

CREATE TEMPORARY FUNCTION ua AS 'by.artsiom.bigdata201.udf.UAGUDF';
```

Here is the hive script to select most popular os, device, and browser for each city:

```sql
ADD JAR /path/to/udf.jar;

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
		FROM bids b1
	) b2
	GROUP BY b2.cityId, b2.os, b2.device, b2.browser
) b3
JOIN sity c ON c.cityId = b3.cityId
WHERE b3.rnk = 1
```

<details><summary>Output</summary>
<p>

```csv
aba     Windows XP      Computer        Internet Explorer 7                                                                                                               
akesu   Windows XP      Computer        Internet Explorer 8                                                                                                                             
alashan Windows XP      Computer        Internet Explorer 8                                                                                                                             
aletai  Windows XP      Computer        Internet Explorer 8                                                                                                                             
ali     Windows XP      Computer        Internet Explorer 7                                                                                                                             
ankang  Windows XP      Computer        Internet Explorer 8                                                                                                                             
anqing  Windows XP      Computer        Internet Explorer 8                                                                                                                             
anshan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
anshun  Windows XP      Computer        Internet Explorer 6                                                                                                                             
anyang  Windows XP      Computer        Internet Explorer 8                                                                                                                             
baicheng        Windows XP      Computer        Internet Explorer 8                                                                                                                     
baise   Windows XP      Computer        Internet Explorer 8                                                                                                                             
baishan Windows XP      Computer        Internet Explorer 8                                                                                                                             
baiyin  Windows XP      Computer        Internet Explorer 8                                                                                                                             
bangbu  Windows XP      Computer        Internet Explorer 7                                                                                                                             
baoding Windows XP      Computer        Internet Explorer 8                                                                                                                             
baoji   Windows XP      Computer        Internet Explorer 7                                                                                                                             
baoshan Windows XP      Computer        Internet Explorer 7                                                                                                                             
baotou  Windows XP      Computer        Internet Explorer 7                                                                                                                             
bayannaoer      Windows XP      Computer        Internet Explorer 7                                                                                                                     
bayinguoleng    Windows XP      Computer        Internet Explorer 8                                                                                                                     
bazhong Windows XP      Computer        Internet Explorer 8                                                                                                                             
beihai  Windows XP      Computer        Internet Explorer 7                                                                                                                             
benxi   Windows XP      Computer        Internet Explorer 7                                                                                                                             
bijie   Windows XP      Computer        Internet Explorer 8                                                                                                                             
binzhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
boertala        Windows XP      Computer        Internet Explorer 8                                                                                                                     
bozhou  Windows XP      Computer        Internet Explorer 7                                                                                                                             
cangzhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
changchun       Windows XP      Computer        Internet Explorer 8                                                                                                                     
changde Windows XP      Computer        Internet Explorer 8                                                                                                                             
changdu Windows XP      Computer        Internet Explorer 7                                                                                                                             
changji Windows XP      Computer        Internet Explorer 8                                                                                                                             
changsha        Windows XP      Computer        Internet Explorer 8                                                                                                                     
changzhi        Windows XP      Computer        Internet Explorer 8                                                                                                                     
changzhou       Windows XP      Computer        Internet Explorer 7                                                                                                                     
chaohu  Windows XP      Computer        Internet Explorer 8                                                                                                                             
chaoyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
chaozhou        Windows XP      Computer        Chrome 21                                                                                                                               
chengde Windows XP      Computer        Internet Explorer 8                                                                                                                             
chengdu Windows XP      Computer        Internet Explorer 8                                                                                                                             
chenzhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
chifeng Windows XP      Computer        Internet Explorer 7                                                                                                                             
chizhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
chongzuo        Windows XP      Computer        Internet Explorer 8                                                                                                                     
chuxiong        Windows XP      Computer        Internet Explorer 7                                                                                                                     
chuzhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
dali    Windows XP      Computer        Internet Explorer 8                                                                                                                             
dalian  Windows XP      Computer        Internet Explorer 8                                                                                                                             
dandong Windows XP      Computer        Internet Explorer 8                                                                                                                             
daqing  Windows XP      Computer        Internet Explorer 8                                                                                                                             
datong  Windows XP      Computer        Internet Explorer 8                                                                                                                             
daxinganling    Windows XP      Computer        Internet Explorer 8                                                                                                                     
dazhou  Windows XP      Computer        Internet Explorer 8                                                                                                                             
dehong  Windows XP      Computer        Chrome 21                                                                                                                                       
deyang  Windows XP      Computer        Internet Explorer 8                                                                                                                             
dezhou  Windows XP      Computer        Internet Explorer 7                                                                                                                             
dingxi  Windows XP      Computer        Internet Explorer 8                                                                                                                             
diqing  Windows XP      Computer        Internet Explorer 6                                                                                                                             
dongguan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
dongying        Windows XP      Computer        Internet Explorer 7                                                                                                                     
eerduosi        Windows XP      Computer        Internet Explorer 8                                                                                                                     
enshishi        Windows XP      Computer        Internet Explorer 8                                                                                                                     
ezhou   Windows XP      Computer        Internet Explorer 7                                                                                                                             
fangchenggang   Windows XP      Computer        Internet Explorer 8                                                                                                                     
foshan  Windows XP      Computer        Internet Explorer 7                                                                                                                             
fushun  Windows XP      Computer        Internet Explorer 7                                                                                                                             
fuxin   Windows XP      Computer        Internet Explorer 8                                                                                                                             
fuyang  Windows XP      Computer        Internet Explorer 7                                                                                                                             
fuzhou_124      Windows XP      Computer        Chrome 21                                                                                                                               
fuzhou_134      Windows XP      Computer        Internet Explorer 8                                                                                                                     
gannan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
ganzhou Windows XP      Computer        Internet Explorer 7                                                                                                                             
ganzi   Windows XP      Computer        Internet Explorer 8                                                                                                                             
guangan Windows XP      Computer        Internet Explorer 8                                                                                                                             
guangyuan       Windows XP      Computer        Internet Explorer 8                                                                                                                     
guangzhou       Windows XP      Computer        Internet Explorer 8                                                                                                                     
guigang Windows XP      Computer        Internet Explorer 8                                                                                                                             
guilin  Windows XP      Computer        Internet Explorer 7                                                                                                                             
guiyang Windows XP      Computer        Internet Explorer 8                                                                                                                             
guoluo  Windows XP      Computer        Internet Explorer 8                                                                                                                             
guyuan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
haerbin Windows XP      Computer        Internet Explorer 8                                                                                                                             
haibei  Windows XP      Computer        Internet Explorer 7                                                                                                                             
haidong Windows XP      Computer        Internet Explorer 8                                                                                                                             
haikou  Windows XP      Computer        Internet Explorer 7                                                                                                                             
hainanzangzu    Windows XP      Computer        Internet Explorer 8                                                                                                                     
haixi   Windows XP      Computer        Internet Explorer 8                                                                                                                             
hami    Windows XP      Computer        Internet Explorer 8                                                                                                                             
handan  Windows XP      Computer        Internet Explorer 7                                                                                                                             
hangzhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
hanzhong        Windows XP      Computer        Internet Explorer 8                                                                                                                     
hebi    Windows XP      Computer        Internet Explorer 8                                                                                                                             
hechi   Windows XP      Computer        Internet Explorer 8                                                                                                                             
hefei   Windows XP      Computer        Internet Explorer 8                                                                                                                             
hegang  Windows XP      Computer        Internet Explorer 8                                                                                                                             
heihe   Windows XP      Computer        Internet Explorer 8                                                                                                                             
hengshui        Windows XP      Computer        Internet Explorer 8                                                                                                                     
hengyang        Windows XP      Computer        Internet Explorer 7                                                                                                                     
hetian  Windows XP      Computer        Internet Explorer 8                                                                                                                             
heyuan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
heze    Windows XP      Computer        Internet Explorer 7                                                                                                                             
hezhou  Windows XP      Computer        Internet Explorer 8                                                                                                                             
honghe  Windows XP      Computer        Internet Explorer 8                                                                                                                             
huaian  Windows XP      Computer        Internet Explorer 8                                                                                                                             
huaibei Windows XP      Computer        Internet Explorer 8                                                                                                                             
huaihua Windows XP      Computer        Internet Explorer 7                                                                                                                             
huainan Windows XP      Computer        Internet Explorer 8                                                                                                                             
huanggang       Windows XP      Computer        Internet Explorer 8                                                                                                                     
huangnan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
huangshan       Windows XP      Computer        Internet Explorer 8                                                                                                                     
huangshi        Windows XP      Computer        Internet Explorer 7                                                                                                                     
huhehaote       Windows XP      Computer        Internet Explorer 7                                                                                                                     
huizhou Windows XP      Computer        Internet Explorer 7                                                                                                                             
huludao Windows XP      Computer        Internet Explorer 8                                                                                                                             
hulunbeier      Windows XP      Computer        Internet Explorer 8                                                                                                                     
huzhou  Windows XP      Computer        Internet Explorer 8                                                                                                                             
jiamusi Windows XP      Computer        Internet Explorer 8                                                                                                                             
jian    Windows XP      Computer        Internet Explorer 8                                                                                                                             
jiangmen        Windows XP      Computer        Internet Explorer 7                                                                                                                     
jiaozuo Windows XP      Computer        Internet Explorer 8                                                                                                                             
jiaxing Windows XP      Computer        Internet Explorer 8                                                                                                                             
jiayuguan       Windows XP      Computer        Internet Explorer 8                                                                                                                     
jieyang Windows XP      Computer        Chrome 21                                                                                                                                       
jilin_city      Windows XP      Computer        Internet Explorer 8                                                                                                                     
jinan   Windows XP      Computer        Internet Explorer 8                                                                                                                             
jinchang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
jincheng        Windows XP      Computer        Internet Explorer 8                                                                                                                     
jingdezhen      Windows XP      Computer        Internet Explorer 8                                                                                                                     
jingmen Windows XP      Computer        Internet Explorer 7                                                                                                                             
jingzhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
jinhua  Windows XP      Computer        Internet Explorer 7                                                                                                                             
jining  Windows XP      Computer        Internet Explorer 8                                                                                                                             
jinzhongshi     Windows XP      Computer        Internet Explorer 8                                                                                                                     
jinzhou Windows XP      Computer        Internet Explorer 7                                                                                                                             
jiujiang        Windows XP      Computer        Internet Explorer 7                                                                                                                     
jiuquan Windows XP      Computer        Internet Explorer 8                                                                                                                             
jixi    Windows XP      Computer        Internet Explorer 8                                                                                                                             
kaifeng Windows XP      Computer        Internet Explorer 7                                                                                                                             
kashi   Windows XP      Computer        Internet Explorer 8                                                                                                                             
kelamayi        Windows XP      Computer        Internet Explorer 7                                                                                                                     
kezilesukeerkezi        Windows XP      Computer        Internet Explorer 8                                                                                                             
kunming Windows XP      Computer        Internet Explorer 7                                                                                                                             
laibin  Windows XP      Computer        Internet Explorer 8                                                                                                                             
laiwu   Windows XP      Computer        Internet Explorer 8                                                                                                                             
langfang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
lanzhou Windows XP      Computer        Internet Explorer 7                                                                                                                             
lasa    Windows XP      Computer        Internet Explorer 7                                                                                                                             
leshan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
liangshan       Windows XP      Computer        Internet Explorer 8                                                                                                                     
lianyungang     Windows XP      Computer        Internet Explorer 7                                                                                                                     
liaocheng       Windows XP      Computer        Internet Explorer 8                                                                                                                     
liaoyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
liaoyuan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
lijiang Windows XP      Computer        Internet Explorer 8                                                                                                                             
lincang Windows XP      Computer        Internet Explorer 7                                                                                                                             
linfen  Windows XP      Computer        Internet Explorer 8                                                                                                                             
linxia  Windows XP      Computer        Internet Explorer 8                                                                                                                             
linyi   Windows XP      Computer        Internet Explorer 7                                                                                                                             
linzhi  Windows XP      Computer        Internet Explorer 7                                                                                                                             
lishui  Windows XP      Computer        Internet Explorer 8                                                                                                                             
liuan   Windows XP      Computer        Internet Explorer 8                                                                                                                             
liupanshui      Windows XP      Computer        Internet Explorer 7                                                                                                                     
liuzhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
longnan Windows XP      Computer        Internet Explorer 7                                                                                                                             
longyan Windows XP      Computer        Internet Explorer 8                                                                                                                             
loudi   Windows XP      Computer        Internet Explorer 7                                                                                                                             
luohe   Windows XP      Computer        Internet Explorer 8                                                                                                                             
luoyang Windows XP      Computer        Internet Explorer 8                                                                                                                             
luzhou  Windows XP      Computer        Internet Explorer 7                                                                                                                             
lvliang Windows XP      Computer        Internet Explorer 8                                                                                                                             
maanshan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
maoming Windows XP      Computer        Internet Explorer 7                                                                                                                             
meishan Windows XP      Computer        Internet Explorer 8                                                                                                                             
meizhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
mianyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
mudanjiang      Windows XP      Computer        Internet Explorer 8                                                                                                                     
nanchang        Windows XP      Computer        Internet Explorer 7                                                                                                                     
nanchong        Windows XP      Computer        Internet Explorer 8                                                                                                                     
nanjing Windows XP      Computer        Internet Explorer 8                                                                                                                             
nanning Windows XP      Computer        Internet Explorer 8                                                                                                                             
nanping Windows XP      Computer        Internet Explorer 8                                                                                                                             
nantong Windows XP      Computer        Internet Explorer 7                                                                                                                             
nanyang Windows XP      Computer        Internet Explorer 8                                                                                                                             
naqu    Windows XP      Computer        Internet Explorer 7                                                                                                                             
neijiang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
ningbo  Windows XP      Computer        Internet Explorer 8                                                                                                                             
ningde  Windows XP      Computer        Chrome 21                                                                                                                                       
nujiang Windows XP      Computer        Chrome 21                                                                                                                                       
panjin  Windows XP      Computer        Internet Explorer 7                                                                                                                             
panzhihua       Windows XP      Computer        Internet Explorer 7                                                                                                                     
pingdingshan    Windows XP      Computer        Internet Explorer 7                                                                                                                     
pingliang       Windows XP      Computer        Internet Explorer 8                                                                                                                     
pingxiang       Windows XP      Computer        Internet Explorer 8                                                                                                                     
puer    Windows XP      Computer        Internet Explorer 8                                                                                                                             
putian  Windows XP      Computer        Internet Explorer 8                                                                                                                             
puyang  Windows XP      Computer        Internet Explorer 8                                                                                                                             
qiandongnan     Windows XP      Computer        Internet Explorer 8                                                                                                                     
qiannan Windows XP      Computer        Internet Explorer 8                                                                                                                             
qianxinan       Windows XP      Computer        Internet Explorer 8                                                                                                                     
qingdao Windows XP      Computer        Internet Explorer 8                                                                                                                             
qingyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
qingyuan        Windows XP      Computer        Chrome 21                                                                                                                               
qinhuangdao     Windows XP      Computer        Internet Explorer 8                                                                                                                     
qinzhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
qiqihaer        Windows XP      Computer        Internet Explorer 8                                                                                                                     
qitaihe Windows XP      Computer        Internet Explorer 8                                                                                                                             
quanzhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
qujing  Windows XP      Computer        Internet Explorer 7                                                                                                                             
quzhou  Windows XP      Computer        Internet Explorer 8                                                                                                                             
rikaze  Windows XP      Computer        Internet Explorer 8                                                                                                                             
rizhao  Windows XP      Computer        Internet Explorer 7                                                                                                                             
sanmenxia       Windows XP      Computer        Internet Explorer 8                                                                                                                     
sanming Windows XP      Computer        Internet Explorer 8                                                                                                                             
sanya   Windows XP      Computer        Internet Explorer 7                                                                                                                             
shangluo        Windows XP      Computer        Internet Explorer 8                                                                                                                     
shangqiu        Windows XP      Computer        Internet Explorer 7                                                                                                                     
shangrao        Windows XP      Computer        Internet Explorer 6                                                                                                                     
shannan Windows XP      Computer        Internet Explorer 7                                                                                                                             
shantou Windows XP      Computer        Chrome 21                                                                                                                                       
shanwei Windows XP      Computer        Chrome 21                                                                                                                                       
shaoguan        Windows XP      Computer        Internet Explorer 7                                                                                                                     
shaoxing        Windows XP      Computer        Internet Explorer 7                                                                                                                     
shaoyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
shenyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
shenzhen        Windows XP      Computer        Internet Explorer 8                                                                                                                     
shijiazhuang    Windows XP      Computer        Internet Explorer 8                                                                                                                     
shiyan  Windows XP      Computer        Internet Explorer 7                                                                                                                             
shizuishan      Windows XP      Computer        Internet Explorer 8                                                                                                                     
shuangyashan    Windows XP      Computer        Internet Explorer 8                                                                                                                     
shuozhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
siping  Windows XP      Computer        Internet Explorer 8                                                                                                                             
songyuan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
suihua  Windows XP      Computer        Internet Explorer 8                                                                                                                             
suining Windows XP      Computer        Internet Explorer 8                                                                                                                             
suizhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
suqian  Windows XP      Computer        Internet Explorer 8                                                                                                                             
suzhou  Windows XP      Computer        Internet Explorer 8                                                                                                                             
suzhou_jiangsu  Windows XP      Computer        Internet Explorer 8                                                                                                                     
tacheng Windows XP      Computer        Internet Explorer 8                                                                                                                             
taian   Windows XP      Computer        Internet Explorer 7                                                                                                                             
taiyuan Windows XP      Computer        Internet Explorer 8                                                                                                                             
taizhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
taizhou_jiangsu Windows XP      Computer        Internet Explorer 8                                                                                                                     
tangshan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
tianshui        Windows XP      Computer        Internet Explorer 8                                                                                                                     
tieling Windows XP      Computer        Internet Explorer 8                                                                                                                             
tonghua Windows XP      Computer        Internet Explorer 8                                                                                                                             
tongliao        Windows XP      Computer        Internet Explorer 7                                                                                                                     
tongling        Windows XP      Computer        Internet Explorer 8                                                                                                                     
tongren Windows XP      Computer        Internet Explorer 8                                                                                                                             
tongzhou        Windows XP      Computer        Internet Explorer 7                                                                                                                     
tulufan Windows XP      Computer        Internet Explorer 8                                                                                                                             
unknown Windows XP      Computer        Internet Explorer 8                                                                                                                             
weifang Windows XP      Computer        Internet Explorer 8                                                                                                                             
weihai  Windows XP      Computer        Internet Explorer 8                                                                                                                             
weinan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
wenshan Windows XP      Computer        Internet Explorer 7                                                                                                                             
wenzhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
wuhai   Windows XP      Computer        Internet Explorer 8                                                                                                                             
wuhan   Windows XP      Computer        Internet Explorer 8                                                                                                                             
wuhu    Windows XP      Computer        Internet Explorer 7                                                                                                                             
wulanchabu      Windows XP      Computer        Internet Explorer 8                                                                                                                     
wulumuqi        Windows XP      Computer        Internet Explorer 8                                                                                                                     
wuwei   Windows XP      Computer        Internet Explorer 8                                                                                                                             
wuxi    Windows XP      Computer        Internet Explorer 8                                                                                                                             
wuzhong Windows XP      Computer        Internet Explorer 8                                                                                                                             
wuzhou  Windows XP      Computer        Internet Explorer 7                                                                                                                             
xiamen  Windows XP      Computer        Internet Explorer 8                                                                                                                             
xian    Windows XP      Computer        Internet Explorer 8                                                                                                                             
xiangfan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
xiangtan        Windows XP      Computer        Internet Explorer 7                                                                                                                     
xiangxi Windows XP      Computer        Internet Explorer 8                                                                                                                             
xianning        Windows XP      Computer        Internet Explorer 8                                                                                                                     
xianyang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
xiaogan Windows XP      Computer        Internet Explorer 8                                                                                                                             
xilinguole      Windows XP      Computer        Internet Explorer 8                                                                                                                     
xingan  Windows XP      Computer        Internet Explorer 8                                                                                                                             
xingtai Windows XP      Computer        Internet Explorer 8                                                                                                                             
xining  Windows XP      Computer        Internet Explorer 7                                                                                                                             
xinxiang        Windows XP      Computer        Internet Explorer 8                                                                                                                     
xinyang Windows XP      Computer        Internet Explorer 7                                                                                                                             
xinyu   Windows XP      Computer        Internet Explorer 6                                                                                                                             
xinzhou Windows XP      Computer        Internet Explorer 8                                                                                                                             
xishuangbanna   Windows XP      Computer        Internet Explorer 8                                                                                                                     
xuancheng       Windows XP      Computer        Internet Explorer 8                                                                                                                     
xuchang Windows XP      Computer        Internet Explorer 8                                                                                                                             
xuzhou  Windows XP      Computer        Internet Explorer 8                                                                                                                             
yaan    Windows XP      Computer        Internet Explorer 8                                                                                                                             
yanan   Windows XP      Computer        Internet Explorer 8                                                                                                                             
yanbian Windows XP      Computer        Internet Explorer 8                                                                                                                             
yancheng        Windows XP      Computer        Internet Explorer 8                                                                                                                     
yangjiang       Windows XP      Computer        Internet Explorer 8                                                                                                                     
yangquan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
yangzhou        Windows XP      Computer        Internet Explorer 7                                                                                                                     
yantai  Windows XP      Computer        Internet Explorer 8                                                                                                                             
yibin   Windows XP      Computer        Internet Explorer 7                                                                                                                             
yichang Windows XP      Computer        Internet Explorer 8                                                                                                                             
yichun_134      Windows XP      Computer        Internet Explorer 8                                                                                                                     
yichun_65       Windows XP      Computer        Internet Explorer 8                                                                                                                     
yili    Windows XP      Computer        Internet Explorer 8                                                                                                                             
yinchuan        Windows XP      Computer        Internet Explorer 7                                                                                                                     
yingkou Windows XP      Computer        Internet Explorer 7                                                                                                                             
yingtan Windows XP      Computer        Internet Explorer 8                                                                                                                             
yiyang  Windows XP      Computer        Internet Explorer 6                                                                                                                             
yongzhou        Windows XP      Computer        Internet Explorer 8                                                                                                                     
yueyang Windows XP      Computer        Internet Explorer 7                                                                                                                             
yulin_238       Windows XP      Computer        Internet Explorer 8                                                                                                                     
yulin_333       Windows XP      Computer        Internet Explorer 8                                                                                                                     
yuncheng        Windows XP      Computer        Internet Explorer 8                                                                                                                     
yunfu   Windows XP      Computer        Internet Explorer 8                                                                                                                             
yushu   Windows XP      Computer        Internet Explorer 7                                                                                                                             
yuxi    Windows XP      Computer        Internet Explorer 7                                                                                                                             
zaozhuang       Windows XP      Computer        Internet Explorer 7                                                                                                                     
zhangjiajie     Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhangjiakou     Windows XP      Computer        Internet Explorer 7                                                                                                                     
zhangye Windows XP      Computer        Internet Explorer 8                                                                                                                             
zhangzhou       Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhanjiang       Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhaoqing        Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhaotong        Windows XP      Computer        Internet Explorer 7                                                                                                                     
zhengzhou       Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhenjiang       Windows XP      Computer        Internet Explorer 7                                                                                                                     
zhongshan       Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhongwei        Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhoukou Windows XP      Computer        Internet Explorer 8                                                                                                                             
zhoushan        Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhuhai  Windows XP      Computer        Internet Explorer 8                                                                                                                             
zhumadian       Windows XP      Computer        Internet Explorer 8                                                                                                                     
zhuzhou Windows XP      Computer        Internet Explorer 7                                                                                                                             
zibo    Windows XP      Computer        Internet Explorer 8                                                                                                                             
zigong  Windows XP      Computer        Internet Explorer 8                                                                                                                             
ziyang  Windows XP      Computer        Internet Explorer 8                                                                                                                             
zunyi   Windows XP      Computer        Internet Explorer 7                                                                                                                             
```

</p>
</details>

### Task 2

Here are the list of a few schemas of partitioning/bucketing for the dataset (the script can be found in the `scripts/partitioned` directory:

 - partitioned by city id and os name
 - partitioned by city id
 - partitioned by region id and city id
 - partitioned by time and bucketed by city

 All of these schemas are appropriate to corresponding tasks and works on the dataset. For example, if we need analyze the data by some time range it is good to use the last schema proposed by me. In case of the `most popular os, device, browser for each city` we should consider only the second one as we are working only with `cityId` and `useragent` columns here and can effectively distribute work for each partition (cityId) among the map/reducers in our cluter. We won't get any execution time advantage by using other schemas, we will only lose. Partitioning by any other column does not make sense because of the large partitions number. We have to remember that too many partitions can lead to performance degradation.

 Plain text data takes 22.6 Gb of disc space while the same dataset in ORC takes only 3.2 Gb. It is huge difference.

We can create an infex for the cityId column by running:

```bash
hive -d INDEX_NAME=cityIdIdx \
	-d TABLE_NAME=bids \
	-d COL_NAME=cityId \
	-f create_index.hql
```
Actually, indexes in Hive are not recommended. The reason for this is ORC. ORC has build in Indexes which allow the format to skip blocks of data during read, they also support Bloom filters. Together this pretty much replicates what Hive Indexes did and they do it automatically in the data format without the need to manage an external table (which is essentially what happens in indexes). Also, ORC allows us to read only columns needed.

Tez offers a reusable, flexible and extensible platform in which random data-flow oriented frameworks are supported, whereas replicated functionalities are avoided. Tez APIs let frameworks model both logical as well as physical semantics of the data flow graphs in a clear manner, with nominal code.
Following significant contributions are made by Apache Tez:
- Permits model computation as a Directed Acyclic Graph (DAG)
- Uncovers APIs to progress the DAG definition in a dynamic way
- Offers an efficient and scalable execution of advanced features

On the basis of different parameters, a number of differences can be observed between Tez and MapReduce as shown in the following table:
|Parameters|Apache Tez|MapReduce|
=================================
Processing Model|specific Map phase and we possibly will have several reduce phases|Prior to the reduce phase, a map phase is always needed by MapReduce
Response time|Low|High
Temporary data storage|More efficient.|Not efficient. After every map and reduce phase, it keeps temporary data into HDFS.
Usage of Hadoop containers|Employ existing containers as well.|Extra containers are needed for further jobs.

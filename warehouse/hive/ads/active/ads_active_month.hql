use ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ads_active_month(
    company       string  COMMENT '公司'
) COMMENT '表1'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/ads/ads_active_month';


select 
    *
from ads.ads_basis_collect;
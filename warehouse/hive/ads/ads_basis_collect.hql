use ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ads_basis_collect(
    company       string  COMMENT '公司'
) COMMENT '表1'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/ads/ads_basis_collect';


select 
    *
from dws.dws_active a;
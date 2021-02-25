use dws;

CREATE EXTERNAL TABLE IF NOT EXISTS dws_active(
    company       string  COMMENT '公司'
) COMMENT '表1'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/dws/dws_active';


select 
    *
from dwd.dwd_app a;
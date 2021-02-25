use dwd;

CREATE EXTERNAL TABLE IF NOT EXISTS dwd_store(
    company       string  COMMENT '公司'
) COMMENT '表1'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/dwd/dwd_store';
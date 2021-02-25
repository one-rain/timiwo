use ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_log_app(
    company       string  COMMENT '公司'
) COMMENT 'ods_log_app'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/ods/ods_log_app';
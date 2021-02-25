USE dwd;

CREATE EXTERNAL TABLE IF NOT EXISTS dws_user (
    plat        string comment '端',
    os          string comment '系统'
) comment '表'
PARTITIONED BY (y int comment '年份', dt string comment '日期')
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/dwd/dws_user';
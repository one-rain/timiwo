use dwd;

CREATE EXTERNAL TABLE IF NOT EXISTS dwd_app(
    company       string  COMMENT '公司'
) COMMENT '表1'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/dwd/dwd_app';


ADD FILE urlencode.py;

INSERT OVERWRITE TABLE dwd_app PARTITION(dt)
SELECT
    *
FROM
    (SELECT
        TRANSFORM(tmp.*)
        USING 'python urlencode.py'
        AS (*)
    FROM ods.ods_log_app tmp
    WHERE dt = '${hivevar:day}'
    ) tb;
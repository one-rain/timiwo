USE dws;

CREATE EXTERNAL TABLE IF NOT EXISTS dws_user_order (
    tag         smallint comment '1:签约，2：解约'
) comment '表'
PARTITIONED BY (y int comment '年份', dt string comment '日期')
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:ossPath}/hive/dws/dws_user_order';

with alias_with_dwd_order as (
  select
    *
  from dwd.dwd_order
)

select  
  *
from alias_with_dwd_order a 
left join
  (select * from dwd.dwd_user)
on (a.id=a.id);
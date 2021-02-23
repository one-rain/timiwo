set hive.exec.dynamic.partition.mode=nonstrict;

USE data_model;

CREATE EXTERNAL TABLE IF NOT EXISTS order_renew_cancel_daily (
    company     string comment 'iqiyi or ssports',
    tag         smallint comment '1:签约，2：解约',
    plat        string comment '端',
    os          string comment '系统',
    user_id     string comment '用户ID',
    product_id  string comment '产品ID',
    product_name    string comment '产品名称',
    happen_time string comment '发生的时间'
) comment '连续包签约/解约每日记录表'
PARTITIONED BY (y int comment '年份', dt string comment '日期')
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'oss://ssports-dataoss.oss-cn-beijing-internal.aliyuncs.com/hive/data_model/order_renew_cancel_daily';


INSERT OVERWRITE TABLE order_renew_cancel_daily PARTITION(y, dt)
select
  t1.company,
  2 as tag,
  t2.plat,
  max(t2.os) as os,
  t1.user_id,
  t1.product_id,
  max(t3.product_name) as product_name,
  min(t1.update_time) as happen_time,
  year('${hivevar:day}') as y,
  '${hivevar:day}' as dt
from
  (select
    'iqiyi' as company,
    user_id,
    product_id,
    to_date(create_time) as create_day,
    min(cancel_time) as update_time
  from
    ssports_shop.coop_member_renew
  where
    to_date(cancel_time)='${hivevar:day}' and status=2
    group by user_id, product_id, to_date(create_time)
  union all
  select
    'xinying' as company,
    user_id,
    product_id,
    to_date(create_time) as create_day,
    min(cancel_time) as update_time
  from
    ssports_shop.bb_renew
  where 
    to_date(cancel_time)='${hivevar:day}' and status=2
    group by user_id, product_id, to_date(create_time)
  ) t1
join
  (select
    company, plat, user_id, product_id, pay_day, max(os) as os
  from
    (select
      company,
      lower(plat) as plat,
      if(lower(plat)='app' and pay_way like 'APPLE%', 'iOS', 'Android') as os,
      user_id,
      to_date(pay_time) as pay_day,
      product_id
    from
      ssports_shop.bb_order_refer
    where
      y>=2019 and order_status='E' and pay_way !='TICKET'
    ) t
  group by company, plat, user_id, product_id, pay_day
  ) t2
on (t1.company=t2.company and t1.user_id=t2.user_id and t1.product_id=t2.product_id and t1.create_day=t2.pay_day)
left join
  ssports_shop.mall_product t3
on(t1.product_id=t3.product_id)
group by
  t1.company,t2.plat,t1.user_id,t1.product_id;


INSERT INTO TABLE order_renew_cancel_daily PARTITION(y, dt)
select
  t1.company,
  1 as tag,
  t2.plat,
  max(t2.os) as os,
  t1.user_id,
  t1.product_id,
  max(t3.product_name) as product_name,
  min(t1.update_time) as happen_time,
  year('${hivevar:day}') as y,
  '${hivevar:day}' as dt
from
  (select
    'iqiyi' as company,
    user_id,
    product_id,
    to_date(create_time) as create_day,
    min(create_time) as update_time
  from
    ssports_shop.coop_member_renew
  where
    to_date(create_time)='${hivevar:day}' and status=1
    group by user_id, product_id, to_date(create_time)
  union all
  select
    'xinying' as company,
    user_id,
    product_id,
    to_date(min(create_time)) as create_day,
    min(create_time) as update_time
  from
    ssports_shop.bb_renew
  where 
    to_date(create_time)='${hivevar:day}' and status=1
    group by user_id, product_id, to_date(create_time)
  ) t1
join
  (select
    company, plat, user_id, product_id, pay_day, max(os) as os
  from
    (select
      company,
      lower(plat) as plat,
      if(lower(plat)='app' and pay_way like 'APPLE%', 'iOS', 'Android') as os,
      user_id,
      to_date(pay_time) as pay_day,
      product_id
    from
      ssports_shop.bb_order_refer
    where
      y>=2019 and order_status='E' and pay_way !='TICKET'
    ) t
  group by company, plat, user_id, product_id, pay_day
  ) t2
on (t1.company=t2.company and t1.user_id=t2.user_id and t1.product_id=t2.product_id and t1.create_day=t2.pay_day)
left join
  ssports_shop.mall_product t3
on(t1.product_id=t3.product_id)
group by
  t1.company,t2.plat,t1.user_id,t1.product_id;

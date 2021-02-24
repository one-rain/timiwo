use ssports_stat;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 2020-11-03 调整为依赖业务字段来判断产品分类
CREATE EXTERNAL TABLE IF NOT EXISTS member_sales_volume_daily1(
    company       string  COMMENT '公司',
    order_num     int  COMMENT '当日订单数',
    income        string  COMMENT '当日收入',
    pay_user_num  int  COMMENT '当日付费用户数',
    arpu          string  COMMENT '当日arpu',
    all_order_num int  COMMENT '历史累计订单数',
    all_income    string  COMMENT '历史累计收入',
    all_pay_user_num int  COMMENT '历史累计付费会员数',
    all_arpu      string  COMMENT '历史累计arpu',
    new_pay_num   int  COMMENT '本日新增付费数',
    dz_pay_num    int  COMMENT '历史累计大众会员月卡付费用户数',
    dz_yue_pay_num int  COMMENT '历史累计大众会员连续包月付费用户数',
    all_vip_user_num  int COMMENT '历史累计体育付费会员数(大众+专业)',
    new_vip_pay_num  int COMMENT '本日新增体育会员数(大众+专业)'
) COMMENT '会员销量日报表1'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'oss://ssports-dataoss.oss-cn-beijing-internal.aliyuncs.com/hive/ssports_stat/member_sales_volume_daily1';


INSERT OVERWRITE TABLE member_sales_volume_daily1 PARTITION(dt)
select
    nvl(company, '合计') as company,
    count(IF(pay_way !='TICKET',order_id,null)) as order_num,
    round(sum(pay_amount),2) as income,
    count(distinct if(pay_amount>0, user_id, null)) as pay_user_num,
    round(sum(pay_amount)/count(distinct if(pay_amount>0, user_id, null)), 1) as arpu,
    0 as all_order_num,
    0 as all_income,
    0 as all_pay_user_num,
    0 as all_arpu,
    0 as new_pay_num,
    0 as dz_pay_num,
    0 as dz_yue_pay_num,
    0 as all_vip_user_num,
    0 as new_vip_pay_num,
    '${hivevar:day}' as dt
from
    ssports_shop.bb_order_refer
where
    ((y=year('${hivevar:day}') and m=month('${hivevar:day}')) or (y=year(date_sub('${hivevar:day}', 2)) and m=month(date_sub('${hivevar:day}', 2))))
    and to_date(pay_time)='${hivevar:day}' and order_status='E'
group by
    company
with rollup;

INSERT INTO TABLE member_sales_volume_daily1 partition(dt)
select
    nvl(company, '合计') as company,
    0 as order_num,
    0 as income,
    0 as pay_user_num,
    0 as arpu,
    count(IF(pay_way !='TICKET',order_id,null)) as all_order_num,
    round(sum(pay_amount), 2) as all_income,
    count(distinct if(pay_amount>0, user_id, null)) as all_pay_user_num,
    round(sum(pay_amount)/count(distinct if(pay_amount>0, user_id, null)),1) as all_arpu,
    0 as new_pay_num,
    count(distinct IF(member_type=1 and is_auto_renew=0 and pay_amount>0, user_id, null)) as dz_pay_num,
    count(distinct IF(member_type=1 and is_auto_renew=1 and pay_amount>0, user_id, null)) as dz_yue_pay_num,
    count(distinct IF(member_type in (1, 2) and pay_amount>0, user_id, null)) as all_vip_user_num,
    0 as new_vip_pay_num,
    '${hivevar:day}' as dt
from
    ssports_shop.bb_order_refer
where
    y>=2019 and to_date(pay_time)>='2019-01-01' and to_date(pay_time)<='${hivevar:day}' and order_status='E'
group by
    company
with rollup;

with all_order as (
    select 
        company,
        order_id,
        user_id,
        order_status,
        pay_amount,
        pay_time,
        pay_way,
        member_type,
        is_auto_renew,
        y, m
    from ssports_shop.bb_order_refer
    where order_status='E' 
)

INSERT INTO TABLE member_sales_volume_daily1 partition(dt)
select
    nvl(t1.company, '合计') as company,
    0 as order_num,
    0 as income,
    0 as pay_user_num,
    0 as arpu,
    0 as all_order_num,
    0 as all_income,
    0 as all_pay_user_num,
    0 as all_arpu,
    count(distinct t1.user_id) as new_pay_num,
    0 as dz_pay_num,
    0 as dz_yue_pay_num,
    0 as all_vip_user_num,
    count(distinct IF(t1.tag=1, t1.user_id, null)) as new_vip_pay_num,
    '${hivevar:day}' as dt
from
    (select
        user_id,company, max(if(member_type in (1, 2), 1, 0)) as tag
    from
        all_order
    where
        ((y=year('${hivevar:day}') and m=month('${hivevar:day}')) or (y=year(date_sub('${hivevar:day}', 2)) and m=month(date_sub('${hivevar:day}', 2))))
        and to_date(pay_time)='${hivevar:day}' and order_status='E' and pay_amount>0
    group by company,user_id
    ) t1
left join
    (select
        user_id,company, max(if(member_type in (1, 2), 1, 0)) as tag
    from
        all_order
    where
        pay_amount>0 and to_date(pay_time)<'${hivevar:day}'
    group by company,user_id
    ) t2
on(t1.user_id=t2.user_id and t1.company=t2.company)
where
    t2.user_id is null
group by
    t1.company
with rollup;

--合并
INSERT OVERWRITE TABLE member_sales_volume_daily1 PARTITION(dt)
select 
    company, 
    max(order_num) as order_num, 
    max(income) as income, 
    max(pay_user_num) as pay_user_num, 
    max(arpu) as arpu, 
    max(all_order_num) as all_order_num,
    max(all_income) as all_income, 
    max(all_pay_user_num) as all_pay_user_num, 
    max(all_arpu) as all_arpu,
    max(new_pay_num) as new_pay_num, 
    max(dz_pay_num) as dz_pay_num,
    max(dz_yue_pay_num) as dz_yue_pay_num, 
    max(all_vip_user_num) as all_vip_user_num,
    max(new_vip_pay_num) as new_vip_pay_num,
    '${hivevar:day}' as dt
from
    ssports_stat.member_sales_volume_daily1
where
    dt='${hivevar:day}'
group by company;
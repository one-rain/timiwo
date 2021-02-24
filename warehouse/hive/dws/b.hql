use ssports_stat;

CREATE VIEW IF NOT EXISTS view_dazhong_monthly_sign_daily (
  sign_order_num     COMMENT '当日大众会员签约订单数',
  renew_num          COMMENT '当日大众会员解约订单数',
  sign_user_num      COMMENT '当日大众会员签约用户量',
  new_sign_user_num  COMMENT '新增大众会员签约用户数(5月)',
  new_renew_user_num COMMENT '新增大众会员签约用户数(1月)',
  new_pro_user_num   COMMENT '新增专业会员签约用户数(1月)',
  golf_income        COMMENT '高尔夫直播赛事收入',
  tennis_income      COMMENT '网球直播赛事收入',
  soccer_income      COMMENT '足球直播赛事收入',
  other_income       COMMENT '直播其他收入',
  total_income       COMMENT '直播收入合计',
  dt                 COMMENT '日期'
) COMMENT '爱奇艺侧签解约、有效用户、赛事收入日报表' AS
select
    a.sign_order_num,
    a.renew_num,
    a.sign_user_num,
    a.new_sign_user_num,
    a.new_renew_user_num,
    a.new_pro_user_num,
    b.golf_income,
    b.tennis_income,
    b.soccer_income,
    b.other_income,
    b.total_income,
    a.dt
from 
    (select
        sign_order_num,
        renew_num,
        sign_user_num,
        new_sign_user_num,
        new_renew_user_num,
        new_pro_user_num,
        dt
    from ssports_stat.dazhong_monthly_sign_daily
    where dt>='2021-01-01' and company='iqiyi'
    ) a 
left join 
    (select
        to_date(pay_time) as dt,
        round(sum(if(order_refer='zhibo' and refer_league_type=3, pay_amount, 0)), 2) as golf_income,
        round(sum(if(order_refer='zhibo' and refer_league_type=2, pay_amount, 0)), 2) as tennis_income,
        round(sum(if(order_refer='zhibo' and refer_league_type=1, pay_amount, 0)), 2) as soccer_income,
        round(sum(if(order_refer='zhibo' and refer_league_type!=1 and refer_league_type!=2 and refer_league_type!=3, pay_amount, 0)), 2) as other_income,
        round(sum(if(order_refer='zhibo', pay_amount, 0)), 2) as total_income
    from ssports_shop.bb_order_refer
    where y>=2021 and company='iqiyi' and lower(pay_way) != 'ticket' and order_status='E'
    group by to_date(pay_time)
    ) b 
on (a.dt=b.dt);
use dws;

CREATE VIEW IF NOT EXISTS dws_view_product (
  a     COMMENT '',
  dt    COMMENT '日期'
) COMMENT 'dws_view_product表' AS
select
    *
from 
    dwd.dwd_product
left join 
    (select
        *
    from dwd.dwd_store
    where statue=1
    ) b 
on (a.dt=b.dt);
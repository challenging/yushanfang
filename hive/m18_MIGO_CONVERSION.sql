INSERT OVERWRITE TABLE pri_result.migo_conversion PARTITION(dt = '${date_ymd}')

SELECT seller_id, cate_id, pay_ord_cnt_new, pay_ord_cnt_old
FROM (
    SELECT seller_id
        ,CASE WHEN cate_level1_id IS NULL THEN -999
            ELSE cate_level1_id END as cate_id
        ,SUM((CASE WHEN is_trade_repeat=0 then pay_ord_cnt ELSE 0 END)) AS pay_ord_cnt_new
        ,SUM((CASE WHEN is_trade_repeat=1 then pay_ord_cnt ELSE 0 END)) AS pay_ord_cnt_old
    FROM (
        SELECT seller_id ,cate_level1_id ,is_trade_repeat ,SUM(alipay_trade_num) AS pay_ord_cnt
        FROM (
            SELECT dt, seller_id ,auction_id ,is_trade_repeat ,alipay_trade_num
            FROM sys_seller_dwb_auction_trade_repeat_trade_d
            WHERE dt >= TO_CHAR(DATEADD(TO_DATE('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
                AND dt <= '${date_ymd}'
        ) t1 LEFT OUTER JOIN (
            SELECT auction_id ,cate_level1_id
            FROM sys_seller_dim_item_online_d
            WHERE dt = '${date_ymd}'
        ) t2 ON t1.auction_id = t2.auction_id
        GROUP BY seller_id,cate_level1_id,is_trade_repeat
    ) t
    GROUP BY seller_id,cate_level1_id
    
    UNION ALL
    
    SELECT seller_id
        ,-1 AS cate_id
        ,SUM((CASE WHEN is_trade_repeat=0 then pay_ord_cnt ELSE 0 END)) AS pay_ord_cnt_new
        ,SUM((CASE WHEN is_trade_repeat=1 then pay_ord_cnt ELSE 0 END)) AS pay_ord_cnt_old
    FROM (
        SELECT seller_id ,is_trade_repeat ,SUM(alipay_trade_num) AS pay_ord_cnt
        FROM (
            SELECT dt, seller_id ,is_trade_repeat ,alipay_trade_num
            FROM sys_seller_dwb_auction_trade_repeat_trade_d
            WHERE dt >= TO_CHAR(DATEADD(TO_DATE('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
                AND dt <= '${date_ymd}'
        ) t1 
        GROUP BY seller_id,is_trade_repeat
    ) t
    GROUP BY seller_id
) a;

INSERT OVERWRITE TABLE pri_result.migo_kpi_group_pi PARTITION (dt='${date_ymd}') 

SELECT seller_id, buyer_id, cate_id, min_paytime, max_paytime, orderCount, trade_amt
FROM (
    SELECT a.seller_id, a.buyer_id,
        CASE WHEN b.cate_level1_id IS NULL THEN -999
            ELSE b.cate_level1_id END AS cate_id,
        min(a.alipay_time) AS min_paytime,
        max(a.alipay_time) AS max_paytime,
        COUNT(DISTINCT a.parent_order_id) as orderCount,
        SUM(a.alipay_trade_amt ) as trade_amt
    FROM (
        SELECT seller_id, platform_buyer_id AS buyer_id, auction_id, alipay_time, parent_order_id, alipay_trade_amt
        FROM sys_seller_dwb_shop_order_d
        WHERE trade_status &2 = 2
            AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -365, 'dd'), 'yyyymmdd')
            AND dt <= '${date_ymd}'
    ) a LEFT OUTER JOIN (
        SELECT seller_id, auction_id, cate_level1_id
        FROM sys_seller_dim_item_online_d
        WHERE dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
    GROUP BY a.seller_id, a.buyer_id, b.cate_level1_id

    UNION ALL

    SELECT seller_id, platform_buyer_id AS buyer_id, -1 AS cate_id, 
        MIN(alipay_time) AS min_paytime, 
        MAX(alipay_time) AS max_paytime, 
        COUNT( DISTINCT parent_order_id) AS orderCount, 
        SUM( alipay_trade_amt ) AS trade_amt
    FROM sys_seller_dwb_shop_order_d
    WHERE trade_status&2 = 2
        AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -365, 'dd'),'yyyymmdd')
        AND dt <= '${date_ymd}'
    GROUP BY seller_id, platform_buyer_id
) a;

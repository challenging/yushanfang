INSERT OVERWRITE TABLE pri_result.migo_item_revenue PARTITION (dt = "${date_ymd}")

SELECT seller_id, auction_id, cate_id, money, amount
FROM (
    SELECT a.seller_id,
        CASE WHEN b.cate_level1_id IS NULL THEN -999
            ELSE b.cate_level1_id END AS cate_id,
        a.auction_id,
        SUM(a.alipay_trade_amt) AS money,
        SUM(a.gmv_auction_num) AS amount
    FROM (
        SELECT seller_id, auction_id, alipay_trade_amt, gmv_auction_num
        FROM sys_seller_dwb_shop_order_d
        WHERE trade_status &2 = 2
            AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
            AND dt <= '${date_ymd}'
    ) a LEFT OUTER JOIN (
        SELECT seller_id, auction_id, cate_level1_id
        FROM sys_seller_dim_item_online_d
        WHERE dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
    GROUP BY a.seller_id, a.auction_id, b.cate_level1_id

    UNION ALL

    SELECT seller_id, -1 AS cate_id, auction_id, SUM(alipay_trade_amt) AS money, SUM(gmv_auction_num) AS amount
    FROM sys_seller_dwb_shop_order_d
    WHERE dt >= to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd')
        AND dt <= '${date_ymd}'
    GROUP BY seller_id, auction_id
) a;

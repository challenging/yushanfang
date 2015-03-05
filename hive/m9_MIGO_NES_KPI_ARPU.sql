INSERT OVERWRITE TABLE pri_result.migo_kpi_4_arpu PARTITION (dt='${date_ymd}')

SELECT seller_id, cate_id, avg_price, avg_price_first, avg_price_repeat
FROM (
    SELECT a.seller_id, a.cate_id, c.avg_price, a.avg_price_first, b.avg_price_repeat
    FROM (
        -- average price by user (first buying)
        SELECT a.seller_id,
            CASE WHEN b.cate_level1_id IS NULL THEN -999
                ELSE b.cate_level1_id END AS cate_id,
            CASE WHEN SUM(a.alipay_winner_num) = 0 THEN 0
                ELSE SUM(a.alipay_trade_amt) / SUM(a.alipay_winner_num) END AS avg_price_first
        FROM (
            SELECT seller_id, auction_id, alipay_trade_amt, alipay_winner_num
            FROM sys_seller_dwb_auction_trade_repeat_trade_d
            WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
                AND is_trade_repeat = 0
        ) a LEFT OUTER JOIN (
            SELECT seller_id, auction_id, cate_level1_id
            FROM sys_seller_dim_item_online_d
            WHERE dt = '${date_ymd}'
        ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
        GROUP BY a.seller_id, b.cate_level1_id
    ) a JOIN (
        --average price by user (repeat)
        SELECT a.seller_id,
            CASE WHEN b.cate_level1_id IS NULL THEN -999
                ELSE b.cate_level1_id END AS cate_id,
            CASE WHEN SUM(a.alipay_winner_num) = 0 THEN 0
                ELSE SUM(a.alipay_trade_amt) / SUM(a.alipay_winner_num) END AS avg_price_repeat
        FROM (
            SELECT seller_id, auction_id, alipay_trade_amt, alipay_winner_num
            FROM sys_seller_dwb_auction_trade_repeat_trade_d
            WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
                AND is_trade_repeat = 1
        ) a LEFT OUTER JOIN (
            SELECT seller_id, auction_id, cate_level1_id
            FROM sys_seller_dim_item_online_d
            WHERE dt = '${date_ymd}'
        ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
        GROUP BY a.seller_id, b.cate_level1_id
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id JOIN (
        --average price by user (all)
        SELECT a.seller_id,
            CASE WHEN b.cate_level1_id IS NULL THEN -999
                ELSE b.cate_level1_id END AS cate_id,
            CASE WHEN SUM(a.alipay_winner_num) = 0 THEN 0
                ELSE SUM(a.alipay_trade_amt) / SUM(a.alipay_winner_num) END AS avg_price
        FROM (
            SELECT seller_id, auction_id, alipay_trade_amt, alipay_winner_num
            FROM sys_seller_dwb_auction_trade_repeat_trade_d
            WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
        ) a LEFT OUTER JOIN (
            SELECT seller_id, auction_id, cate_level1_id
            FROM sys_seller_dim_item_online_d
            WHERE dt = '${date_ymd}'
        ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
        GROUP BY a.seller_id, b.cate_level1_id
    ) c ON a.seller_id = c.seller_id AND a.cate_id = c.cate_id
    
    UNION ALL
    
    SELECT a.seller_id, -1 AS cate_id, c.avg_price, a.avg_price_first, b.avg_price_repeat
    FROM (
        -- average price by user (first buying)
        SELECT seller_id
        , CASE WHEN SUM(alipay_winner_num) = 0 THEN 0
            ELSE SUM(alipay_trade_amt) / SUM(alipay_winner_num) END AS avg_price_first
        FROM sys_seller_dwb_auction_trade_repeat_trade_d
        WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
        AND is_trade_repeat = 0
        GROUP BY seller_id
    ) a
    JOIN (
        --average price by user (repeat)
        SELECT seller_id
        , CASE WHEN SUM(alipay_winner_num) = 0 THEN 0
            ELSE SUM(alipay_trade_amt) / SUM(alipay_winner_num) END AS avg_price_repeat
        FROM  sys_seller_dwb_auction_trade_repeat_trade_d
        WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
        AND is_trade_repeat = 1
        GROUP BY seller_id
    ) b
    ON a.seller_id = b.seller_id
    JOIN (
        --average price by user (all)
        SELECT seller_id
        , CASE WHEN SUM(alipay_winner_num) = 0 THEN 0
            ELSE SUM(alipay_trade_amt) / SUM(alipay_winner_num) END AS avg_price
        FROM  sys_seller_dwb_auction_trade_repeat_trade_d
        WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
        GROUP BY seller_id
    ) c
    ON a.seller_id = c.seller_id
) a;

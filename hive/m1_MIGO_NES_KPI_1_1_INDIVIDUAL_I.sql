INSERT OVERWRITE TABLE pri_result.migo_kpi_1_individual_pi PARTITION (dt='${date_ymd}') 

SELECT seller_id, buyer_id, cate_id, diff, alipay_time, pre_alipay_time, alipay_time_seq
FROM (
    SELECT seller_id, buyer_id, cate_id
        , datediff( to_date(alipay_time,'yyyy-mm-dd'), to_date(pre_alipay_time,'yyyy-mm-dd'), "dd") AS diff
        , alipay_time, pre_alipay_time
        , RANK() over(PARTITION BY seller_id, buyer_id, cate_id ORDER BY alipay_time) AS alipay_time_seq
    FROM (
        SELECT seller_id ,buyer_id , cate_id, alipay_time
        ,lead(alipay_time, 1, "0") over(PARTITION BY seller_id, buyer_id, cate_id ORDER BY alipay_time DESC) AS pre_alipay_time
        FROM (
            SELECT a.seller_id, a.buyer_id,
                CASE WHEN b.cate_level1_id IS NULL THEN -999
                    ELSE b.cate_level1_id END AS cate_id,
                to_char(a.alipay_time,'yyyy-mm-dd') AS alipay_time
            FROM (
                SELECT seller_id, platform_buyer_id AS buyer_id, auction_id, alipay_time
                FROM ali_seller_dwb_shop_order_d_new
                WHERE trade_status &2 = 2
                    AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -365, 'dd'), 'yyyymmdd')
                    AND dt <= '${date_ymd}'
            ) a LEFT OUTER JOIN (
                SELECT seller_id, auction_id, cate_level1_id
                FROM ali_seller_dim_item_online_d
                WHERE dt = '${date_ymd}'
            ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
        ) a
    ) a
    WHERE a.alipay_time != a.pre_alipay_time

    UNION ALL

    SELECT seller_id, buyer_id, -1 AS cate_id
        , datediff( to_date(alipay_time,'yyyy-mm-dd'), to_date(pre_alipay_time,'yyyy-mm-dd'), "dd") AS diff
        , alipay_time, pre_alipay_time
        , RANK() over(partition by seller_id, buyer_id ORDER BY alipay_time) AS alipay_time_seq
    FROM (
        SELECT seller_id ,buyer_id ,alipay_time
        ,lead(alipay_time, 1, "0") over(PARTITION BY seller_id, buyer_id ORDER BY alipay_time DESC) as pre_alipay_time
        FROM (
            SELECT seller_id, platform_buyer_id AS buyer_id, to_char(alipay_time,'yyyy-mm-dd') as alipay_time
            FROM ali_seller_dwb_shop_order_d_new
            WHERE trade_status &2 = 2
                AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -365, 'dd'), 'yyyymmdd')
                AND dt <= '${date_ymd}'
        ) b
    ) a
    WHERE a.alipay_time != a.pre_alipay_time
) a;


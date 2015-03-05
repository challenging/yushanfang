INSERT OVERWRITE TABLE pri_result.migo_stickiness PARTITION (dt='${date_ymd}')

SELECT a.seller_id, a.cate_id, a.tagging, b.userCount_in30, a.totaluserCount_in30
FROM (
    -- 計算過去三十天內且有消費的會員，並依條件貼標籤計算標籤人數<分母>
    SELECT a.seller_id AS seller_id, a.cate_id AS cate_id, a.tagging AS tagging, COUNT(a.tagging) AS totaluserCount_in30
    FROM (
        SELECT seller_id, buyer_id, cate_id
        , CASE WHEN (MAX(alipay_time_seq)=2) THEN 'EB2'
               WHEN (MAX(alipay_time_seq)>2) THEN '3TIMES'
               ELSE '1TIME' END as tagging
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}'
        AND to_date(alipay_time,'yyyy-mm-dd') >= dateadd(to_date('${date_ymd}', 'yyyymmdd'), -30 , 'dd')
        GROUP BY seller_id, buyer_id, cate_id
    ) a GROUP BY a.seller_id, a.tagging, a.cate_id
) a LEFT OUTER JOIN (
    -- 拿取商家內每個買家最後購買間隔，若無，設定一個非常大的數字
    SELECT m.seller_id AS seller_id, m.cate_id AS cate_id, COUNT(DISTINCT e.buyer_id) AS userCount_in30, tagging AS tagging
    FROM (
         -- 拿取每個商家的 Group Mean
        SELECT DISTINCT seller_id, cate_id, g_gap_mean
        FROM pri_result.migo_NES_gap_mean_v2 ---pri_result.migo_kpi_1_adj_gap_mean
        WHERE dt = '${date_ymd}'
    ) m JOIN (
        SELECT a.seller_id, a.buyer_id, a.cate_id, purchasing_interval AS last_purchasing_interval, tagging
        FROM pri_result.migo_kpi_1_individual_pi a
        JOIN (
            SELECT seller_id, buyer_id, cate_id, MAX(alipay_time_seq) as maxSEQ
                , CASE WHEN (MAX(alipay_time_seq) = 2) THEN 'EB2'
                       WHEN (MAX(alipay_time_seq) > 2) THEN '3TIMES'
                       ELSE '1TIME' END as tagging
            FROM pri_result.migo_kpi_1_individual_pi a
            WHERE dt = '${date_ymd}'
            AND to_date(alipay_time,'yyyy-mm-dd') >= dateadd(to_date('${date_ymd}', 'yyyymmdd'), -30 , 'dd')
            GROUP BY seller_id, buyer_id, cate_id
        ) c ON a.seller_id = c.seller_id AND a.buyer_id = c.buyer_id AND a.cate_id = c.cate_id AND a.alipay_time_seq = c.maxSEQ
        WHERE a.purchasing_interval IS NOT NULL
    ) e ON m.seller_id = e.seller_id AND m.cate_id = e.cate_id
    WHERE last_purchasing_interval < g_gap_mean
    GROUP BY m.seller_id, tagging, m.cate_id
) b ON a.seller_id = b.seller_id AND a.tagging = b.tagging AND a.cate_id = b.cate_id;

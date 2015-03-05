INSERT OVERWRITE TABLE pri_result.migo_active PARTITION (dt='${date_ymd}')

SELECT a.seller_id, a.cate_id, a.tagging, active_count, total_count
--=================== 計算 Acitve Rate ======================================
FROM (
    -- 計算每個標籤人數
    SELECT seller_id, cate_id, tagging, COUNT(buyer_id) AS total_count
    FROM pri_result.migo_tagging_nes
    WHERE dt = '${date_ymd}'
    GROUP BY seller_id, cate_id, tagging
) a LEFT OUTER JOIN (
    -- 計算過去三十天內，依照每個 TAG 分門別類，計算訂單會員數量
    SELECT a.seller_id, a.cate_id, c.tagging, COUNT(DISTINCT a.buyer_id) AS active_count
    FROM (
        -- 取得商家每個客戶的消費日期
        SELECT seller_id, buyer_id, cate_id, alipay_time
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}'
    ) a JOIN (
        -- 取得商家的 Group Mean
        SELECT DISTINCT seller_id, buyer_id, cate_id, g_gap_mean
        FROM pri_result.migo_NES_gap_mean_v2 ---pri_result.migo_kpi_1_adj_gap_mean
        WHERE dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id JOIN (
        -- 取個商家每個會員的 NES 標籤
        SELECT seller_id, buyer_id, cate_id, tagging
        FROM pri_result.migo_tagging_nes
        WHERE dt = '${date_ymd}'
    ) c ON a.seller_id = c.seller_id AND a.buyer_id = c.buyer_id AND a.cate_id = c.cate_id
    WHERE to_date(a.alipay_time,'yyyy-mm-dd') >= dateadd(to_date('${date_ymd}', 'yyyymmdd'), -b.g_gap_mean , 'dd')
    GROUP BY a.seller_id, a.cate_id, c.tagging
) b ON a.seller_id = b.seller_id AND a.tagging = b.tagging AND a.cate_id = b.cate_id

INSERT OVERWRITE TABLE pri_result.migo_kpi PARTITION (dt='${date_ymd}')

SELECT a.seller_id,
    a.cate_id, i.cate_level1_name,
    -- 營業額
    CASE WHEN a.revenue IS NULL THEN 0
        ELSE a.revenue END AS revenue,           
    -- 加總第一次訂單的總金額
    CASE WHEN aa.first_total_amount IS NULL THEN 0
        ELSE aa.first_total_amount END AS first_total_amount,   
    -- 有效客戶數
    CASE WHEN b.AM IS NULL THEN 0
        ELSE b.AM END AS active_member, 
    -- 新增首購客戶數
    CASE WHEN e.increasedN IS NULL THEN 0
        ELSE e.increasedN END AS increasedN,
    -- 新增沉睡客戶數
    CASE WHEN g.increasedS IS NULL THEN 0
        ELSE g.increasedS END AS increasedS,       
    -- S3客戶數
    CASE WHEN g.curr IS NULL THEN 0
        ELSE g.curr END AS curr,             
    -- 客戶新增率
    CASE WHEN e.increasedN_ratio IS NULL THEN 0
        ELSE e.increasedN_ratio END AS increasedN_ratio, 
    -- 客戶流失率
    CASE WHEN g.increasedS_ratio IS NULL THEN 0
        ELSE g.increasedS_ratio END AS increasedS_ratio, 
    -- 整體客戶活躍度
    CASE WHEN c.active_radio IS NULL THEN 0
        ELSE c.active_radio END AS active_ratio,    
    -- 老客活躍度
    CASE WHEN c.active_radio_E = 0 THEN 0            
        ELSE c.active_radio_E END AS active_ratio_E,   
    -- 老客活躍數
    CASE WHEN h.active_count_E IS NULL THEN 0   
        ELSE h.active_count_E END AS active_count,
    -- 黏著度
    CASE WHEN c.stickiness_radio IS NULL THEN 0
        ELSE c.stickiness_radio END AS stickiness_ratio, 
    -- 新客黏著度
    CASE WHEN c.stickiness_radio_N IS NULL THEN 0
        ELSE c.stickiness_radio_N END AS stickiness_ratio_N, 
    -- 老客黏著度
    CASE WHEN c.stickiness_radio_E IS NULL THEN 0
        ELSE c.stickiness_radio_E END AS stickiness_ratio_E, 
    -- 有效客單價
    CASE WHEN d.arpu IS NULL THEN 0
        ELSE d.arpu END AS arpu,             
    -- 新客客單價
    CASE WHEN d.arpu_first IS NULL THEN 0
        ELSE d.arpu_first END AS arpu_first,       
    -- 老客客單價
    CASE WHEN d.arpu_repeated IS NULL THEN 0
        ELSE d.arpu_repeated END AS arpu_repeated,    
    -- 網站拜訪次數
    CASE WHEN (j.login_mbr_pv + j.non_login_mbr_pv) IS NULL THEN 0
        ELSE j.login_mbr_pv + j.non_login_mbr_pv END AS mbr_pv, 
    -- 會員拜訪次數
    CASE WHEN j.login_mbr_pv IS NULL THEN 0
        ELSE j.login_mbr_pv END AS login_mbr_pv,                      
    -- 訪客拜訪次數
    CASE WHEN j.non_login_mbr_pv IS NULL THEN 0
        ELSE j.non_login_mbr_pv END AS non_login_mbr_pv,                  
    -- 整體轉換率
    CASE WHEN (j.login_mbr_pv + j.non_login_mbr_pv) = 0 OR (j.login_mbr_pv + j.non_login_mbr_pv) IS NULL OR (k.pay_ord_cnt_new + k.pay_ord_cnt_old) IS NULL THEN -999 
        ELSE (k.pay_ord_cnt_new + k.pay_ord_cnt_old) / (j.login_mbr_pv + j.non_login_mbr_pv) END AS pay_ord_conversion, 
    -- 首購轉換率
    CASE WHEN j.non_login_mbr_pv IS NULL OR j.non_login_mbr_pv = 0 THEN -999                          
        ELSE k.pay_ord_cnt_new / j.non_login_mbr_pv END AS pay_ord_conversion_new,     
    -- 老客轉換率
    CASE WHEN j.login_mbr_pv IS NULL OR j.login_mbr_pv = 0 THEN -999              
        ELSE k.pay_ord_cnt_old / j.login_mbr_pv END AS pay_ord_conversion_old           
FROM (
    SELECT seller_id, cate_id, SUM(sales) AS revenue
    FROM pri_result.migo_item_revenue
    WHERE dt = '${date_ymd}'
    GROUP BY seller_id, cate_id
) a LEFT OUTER JOIN (
    -- 加總第一次訂單的總金額(過去三十天)
    SELECT d.seller_id, d.cate_id, SUM(d.alipay_trade_amt) as first_total_amount
    FROM (
        SELECT a.seller_id, a.buyer_id,
            CASE WHEN b.cate_level1_id IS NULL THEN -999
                ELSE b.cate_level1_id END AS cate_id
            , a.alipay_trade_amt, alipay_time
        FROM (
            SELECT seller_id, platform_buyer_id AS buyer_id, auction_id, alipay_trade_amt, TO_CHAR(alipay_time,'yyyy-mm-dd') AS alipay_time
            FROM sys_seller_dwb_shop_order_d
            WHERE trade_status&2 = 2 AND dt >= TO_CHAR(dateadd(TO_DATE('${date_ymd}', 'yyyymmdd'), -30 , 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
        ) a LEFT OUTER JOIN (
            SELECT seller_id, auction_id, cate_level1_id
            FROM sys_seller_dim_item_online_d
            WHERE dt = '${date_ymd}'
        ) b ON a.seller_id = b.seller_id AND a.auction_id = b.auction_id
    ) d JOIN (
        SELECT seller_id, buyer_id, cate_id, tagging
        FROM pri_result.migo_tagging_nes n
        WHERE dt = '${date_ymd}' AND tagging IN ('N0', 'EB') --過去30天新購買的用戶
    ) n ON d.seller_id = n.seller_id AND d.buyer_id = n.buyer_id AND d.cate_id = n.cate_id JOIN (
        SELECT seller_id, buyer_id, cate_id, alipay_time
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}' AND alipay_time_seq = 1 --第一次購買的日期
    ) p ON d.seller_id = p.seller_id AND d.buyer_id = p.buyer_id AND d.alipay_time = p.alipay_time AND d.cate_id = p.cate_id
    GROUP BY d.seller_id, d.cate_id

    UNION ALL

    SELECT d.seller_id, -1 AS cate_id, SUM(d.alipay_trade_amt) as first_total_amount
    FROM (
        SELECT seller_id, platform_buyer_id AS buyer_id, alipay_trade_amt, TO_CHAR(alipay_time,'yyyy-mm-dd') AS alipay_time
        FROM sys_seller_dwb_shop_order_d
        WHERE trade_status&2 = 2 
            AND dt >= TO_CHAR(DATEADD(TO_DATE('${date_ymd}', 'yyyymmdd'), -30 , 'dd'), "yyyymmdd") 
            AND dt <= '${date_ymd}'
    ) d JOIN (
        SELECT seller_id, buyer_id, tagging
        FROM pri_result.migo_tagging_nes
        WHERE dt = '${date_ymd}' AND cate_id = -1
        AND tagging IN ('N0', 'EB') --過去30天新購買的用戶
    ) n ON d.seller_id = n.seller_id AND d.buyer_id = n.buyer_id JOIN (
        SELECT seller_id, buyer_id, alipay_time
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}' AND cate_id = -1
        AND alipay_time_seq = 1 --第一次購買的日期
    ) p ON d.seller_id = p.seller_id AND d.buyer_id = p.buyer_id AND d.alipay_time = p.alipay_time
    GROUP BY d.seller_id
) aa ON a.seller_id = aa.seller_id AND a.cate_id = aa.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_total) AS AM
    FROM pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging IN ("N0", "EB", "E0")
    GROUP BY seller_id, cate_id
) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, active_radio, active_radio_E, stickiness_radio, stickiness_radio_N, stickiness_radio_E
    FROM pri_result.migo_nes_active_results
    WHERE dt = '${date_ymd}'
) c ON a.seller_id = c.seller_id AND a.cate_id = c.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_total) AS active_count_E
    FROM pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging = 'E0'
    GROUP BY seller_id, cate_id
) h ON a.seller_id = h.seller_id AND a.cate_id = h.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, arpu, arpu_first, arpu_repeated
    FROM pri_result.migo_kpi_4_arpu_slope
    WHERE dt = '${date_ymd}'
) d ON a.seller_id = d.seller_id AND a.cate_id = d.cate_id LEFT OUTER JOIN (
    SELECT a.seller_id, a.cate_id,
        CASE WHEN existing+new = 0 THEN -999 ELSE new/(new+existing) END AS increasedN_ratio,
        CASE WHEN new = 0 THEN -999 ELSE new END AS increasedN
    FROM (
        SELECT seller_id, cate_id, SUM(count_total) AS new
        FROM pri_result.migo_active
        WHERE dt = '${date_ymd}' AND tagging IN ("N0", "EB")
        GROUP BY seller_id, cate_id
    ) a LEFT OUTER JOIN (
        SELECT seller_id, cate_id, SUM(count_total) AS existing
        FROM pri_result.migo_active
        WHERE dt = '${date_ymd}' AND tagging IN ("E0")
        GROUP BY seller_id, cate_id
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id
) e ON a.seller_id = e.seller_id AND a.cate_id = e.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_total) AS S3
    FROM  pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging IN ("S3")
    GROUP BY seller_id, cate_id
) f ON a.seller_id = f.seller_id AND a.cate_id = f.cate_id LEFT OUTER JOIN (
    SELECT a.seller_id, a.cate_id, curr,
        CASE WHEN (pre IS NULL) THEN -999 ELSE (curr - pre)/active END AS increasedS_ratio,
        CASE WHEN (pre IS NULL) THEN -999 ELSE (curr - pre) END AS increasedS
    FROM (
        SELECT seller_id, cate_id, SUM(count_total) AS curr
        FROM pri_result.migo_active
        WHERE dt = '${date_ymd}' AND tagging IN ("S3")
        GROUP BY seller_id, cate_id
    ) a LEFT OUTER JOIN (
        SELECT seller_id, cate_id, SUM(count_total) AS pre
        FROM pri_result.migo_active
        WHERE dt = to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -1, 'dd'), 'yyyymmdd')
            AND tagging IN ("S3")
        GROUP BY seller_id, cate_id
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id LEFT OUTER JOIN (
        SELECT seller_id, cate_id, SUM(count_total) AS active
        FROM pri_result.migo_active
        WHERE dt = '${date_ymd}' AND tagging IN ("N0", "EB", "E0")
        GROUP BY seller_id, cate_id
    ) c ON a.seller_id = c.seller_id AND a.cate_id = c.cate_id
) g ON a.seller_id = g.seller_id AND a.cate_id = g.cate_id LEFT OUTER JOIN (
    SELECT DISTINCT cate_level1_id, cate_level1_name
    FROM sys_seller_dim_cate
    WHERE dt = '${date_ymd}'
) i ON a.cate_id = i.cate_level1_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, login_mbr_pv, non_login_mbr_pv
    FROM pri_result.migo_traffic
    WHERE dt = '${date_ymd}'
) j ON a.seller_id = j.seller_id AND a.cate_id = j.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, pay_ord_cnt_new, pay_ord_cnt_old
    FROM pri_result.migo_conversion
    WHERE dt = '${date_ymd}'
) k ON a.seller_id = k.seller_id AND a.cate_id = k.cate_id;


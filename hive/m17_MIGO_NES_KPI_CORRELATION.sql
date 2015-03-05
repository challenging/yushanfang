INSERT OVERWRITE TABLE pri_result.migo_kpi_cc PARTITION (dt="${date_ymd}")

SELECT a.seller_id
    , a.cate_id
    , CASE WHEN a.cate_id = -1 THEN "無分類目"
        WHEN a.cate_id = -999 THEN "對應不到類目"
        ELSE a.cate_level1_name END
    , 0 -- Revenue 的相關係數
    , 0 -- 加總第一次訂單金額的總數的相關係數
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.MEMBER_Y) = 0 OR SUM(a.MEMBER_Y) IS NULL THEN -999
        ELSE SUM(a.MEMBER_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.MEMBER_Y))) END AS member_cc

    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.MEMBER_NEW_NUM_Y) = 0 OR SUM(a.MEMBER_NEW_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.MEMBER_NEW_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.MEMBER_NEW_NUM_Y))) END AS member_nn_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.MEMBER_SLEEP_NUM_Y) = 0 OR SUM(a.MEMBER_SLEEP_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.MEMBER_SLEEP_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.MEMBER_SLEEP_NUM_Y))) END AS member_sn_cc
    , 0 -- S3客戶數目
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.MEMBER_NEW_RATIO_Y) = 0 OR SUM(a.MEMBER_NEW_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.MEMBER_NEW_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.MEMBER_NEW_RATIO_Y))) END AS member_nr_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.MEMBER_SLEEP_RATIO_Y) = 0 OR SUM(a.MEMBER_SLEEP_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.MEMBER_SLEEP_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.MEMBER_SLEEP_RATIO_Y))) END AS member_sr_cc

    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.ACTIVE_RATIO_Y) = 0 OR SUM(a.ACTIVE_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.ACTIVE_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.ACTIVE_RATIO_Y))) END AS active_r_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.ACTIVE_OLD_RATIO_Y) = 0 OR SUM(a.ACTIVE_OLD_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.ACTIVE_OLD_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.ACTIVE_OLD_RATIO_Y))) END AS active_or_cc

    -- 尚未確定（活躍老客戶數）
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.ACTIVE_OLD_NUM_Y) = 0 OR SUM(a.ACTIVE_OLD_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.ACTIVE_OLD_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.ACTIVE_OLD_NUM_Y))) END AS active_oor_cc

    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.STICKINESS_RATIO_Y) = 0 OR SUM(a.STICKINESS_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.STICKINESS_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.STICKINESS_RATIO_Y))) END AS stickiness_r_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.STICKINESS_NES_RATIO_Y) = 0 OR SUM(a.STICKINESS_NES_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.STICKINESS_NES_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.STICKINESS_NES_RATIO_Y))) END AS stickiness_nr_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.STICKINESS_OLD_RATIO_Y) = 0 OR SUM(a.STICKINESS_OLD_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.STICKINESS_OLD_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.STICKINESS_OLD_RATIO_Y))) END AS stickiness_or_cc

    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.ARPU_NUM_Y) = 0 OR SUM(a.ARPU_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.ARPU_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.ARPU_NUM_Y))) END AS arpu_n_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.ARPU_NEW_NUM_Y) = 0 OR SUM(a.ARPU_NEW_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.ARPU_NEW_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.ARPU_NEW_NUM_Y))) END AS arpu_nn_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 OR SUM(a.ARPU_OLD_NUM_Y) = 0 OR SUM(a.ARPU_OLD_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.ARPU_OLD_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.ARPU_OLD_NUM_Y))) END AS arpu_on_cc

    , CASE WHEN SUM(a.REVENUE_X) = 0 or SUM(a.VISITOR_NUM_Y) = 0 OR SUM(a.VISITOR_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.VISITOR_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.VISITOR_NUM_Y))) END AS visitor_n_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 or SUM(a.VISITOR_OLD_NUM_Y) = 0 OR SUM(a.VISITOR_OLD_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.VISITOR_OLD_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.VISITOR_OLD_NUM_Y))) END AS visiior_on_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 or SUM(a.VISITOR_NEW_NUM_Y) = 0 OR SUM(a.VISITOR_NEW_NUM_Y) IS NULL THEN -999
        ELSE SUM(a.VISITOR_NEW_NUM_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.VISITOR_NEW_NUM_Y))) END AS visitor_nn_cc

    , CASE WHEN SUM(a.REVENUE_X) = 0 or SUM(a.CONVERSION_RATIO_Y) = 0 OR SUM(a.CONVERSION_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.CONVERSION_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.CONVERSION_RATIO_Y))) END AS conversion_r_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 or SUM(a.CONVERSION_NEW_RATIO_Y) = 0 OR SUM(a.CONVERSION_NEW_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.CONVERSION_NEW_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.CONVERSION_NEW_RATIO_Y))) END AS conversion_nr_cc
    , CASE WHEN SUM(a.REVENUE_X) = 0 or SUM(a.CONVERSION_OLD_RATIO_Y) = 0 OR SUM(a.CONVERSION_OLD_RATIO_Y) IS NULL THEN -999
        ELSE SUM(a.CONVERSION_OLD_RATIO_CHILD) / (SQRT(SUM(a.REVENUE_X)) * SQRT(SUM(a.CONVERSION_OLD_RATIO_Y))) END AS conversion_or_cc
FROM (
    SELECT a.dt, a.seller_id, a.cate_id, c.cate_level1_name,
        -- a.member_num, b.R, b.MM,
        a.revenue_num,
        POW((a.revenue_num - b.R), 2) AS REVENUE_X,

        -- 有效客戶數
        (a.revenue_num - b.R) * (member_num - b.AM)     AS MEMBER_CHILD,
        POW((a.member_num - b.AM), 2) AS MEMBER_Y,
        -- 新增客戶數
        (a.revenue_num - b.R) * (a.member_new_num - b.AAN)      AS MEMBER_NEW_NUM_CHILD,
        POW((a.member_new_num - b.AAN), 2)  AS MEMBER_NEW_NUM_Y,
        -- 流失客戶數
        (a.revenue_num - b.R) * (a.member_sleep_num - b.ASN)     AS MEMBER_SLEEP_NUM_CHILD,
        POW((a.member_sleep_num - b.ASN), 2) AS MEMBER_SLEEP_NUM_Y,

        -- 客戶新增率
        (a.revenue_num - b.R) * (a.member_new_ratio - b.ANR)     AS MEMBER_NEW_RATIO_CHILD,
        POW((a.member_new_ratio - b.ANR), 2) AS MEMBER_NEW_RATIO_Y,
         -- 客戶流失率
        (a.revenue_num - b.R) * (a.member_sleep_ratio - b.ASR)     AS MEMBER_SLEEP_RATIO_CHILD,
        POW((a.member_sleep_ratio - b.ASR), 2) AS MEMBER_SLEEP_RATIO_Y,

        -- 整體客戶活躍度
        (a.revenue_num - b.R) * (a.active_ratio - b.AR)     AS ACTIVE_RATIO_CHILD,
        POW((a.active_ratio - b.AR), 2) AS ACTIVE_RATIO_Y,
        -- 老客戶活躍度
        (a.revenue_num - b.R) * (a.active_old_ratio - b.AOR)     AS ACTIVE_OLD_RATIO_CHILD,
        POW((a.active_old_ratio - b.AOR), 2) AS ACTIVE_OLD_RATIO_Y,

        -- 尚未確定（活躍老客戶數）
        (a.revenue_num - b.R) * (a.active_old_num - b.AON) AS ACTIVE_OLD_NUM_CHILD,
        POW((a.active_old_num - b.AON), 2) AS ACTIVE_OLD_NUM_Y,

        -- 黏著度
        (a.revenue_num - b.R) * (a.stickiness_ratio - b.SR)     AS STICKINESS_RATIO_CHILD,
        POW((a.stickiness_ratio - b.SR), 2) AS STICKINESS_RATIO_Y,
        -- 新客黏著度
        (a.revenue_num - b.R) * (a.stickiness_new_ratio - b.SNR)     AS STICKINESS_NES_RATIO_CHILD,
        POW((a.stickiness_new_ratio - b.SNR), 2) AS STICKINESS_NES_RATIO_Y,
        -- 老客黏著度
        (a.revenue_num - b.R) * (a.stickiness_old_ratio - b.SOR)     AS STICKINESS_OLD_RATIO_CHILD,
        POW((a.stickiness_old_ratio - b.SOR), 2) AS STICKINESS_OLD_RATIO_Y,

        -- 有效客單價
        (a.revenue_num - b.R) * (a.arpu_num - b.AN)     AS ARPU_NUM_CHILD,
        POW((a.arpu_num - b.AN), 2) AS ARPU_NUM_Y,
        -- 首購客單價
        (a.revenue_num - b.R) * (a.arpu_new_num - b.ANN)     AS ARPU_NEW_NUM_CHILD,
        POW((a.arpu_new_num - b.ANN), 2) AS ARPU_NEW_NUM_Y,
        -- 重複客單價
        (a.revenue_num - b.R) * (a.arpu_old_num - b.AOON)     AS ARPU_OLD_NUM_CHILD,
        POW((a.arpu_old_num - b.AOON), 2) AS ARPU_OLD_NUM_Y,

        -- 所有客戶拜訪數
        (a.revenue_num - b.R) * (a.visitor_num - b.VN)      AS VISITOR_NUM_CHILD,
        POW((a.visitor_num - b.VN), 2)  AS VISITOR_NUM_Y,
        -- 已成交訪客拜訪次數
        (a.revenue_num - b.R) * (a.visitor_old_num - b.VON)     AS VISITOR_OLD_NUM_CHILD,
        POW((a.visitor_old_num - b.VON), 2) AS VISITOR_OLD_NUM_Y,
        -- 未成交訪客拜訪數
        (a.revenue_num - b.R) * (a.visitor_new_num - b.VNN)     AS VISITOR_NEW_NUM_CHILD,
        POW((a.visitor_new_num - b.VNN), 2) AS VISITOR_NEW_NUM_Y,

        -- 整體轉化率
        (a.revenue_num - b.R) * (a.conversion_ratio - b.CR)     AS CONVERSION_RATIO_CHILD,
        POW((a.conversion_ratio - b.CR), 2) AS CONVERSION_RATIO_Y,
        -- 首購轉化率
        (a.revenue_num - b.R) * (a.conversion_new_ratio - b.CNR)     AS CONVERSION_NEW_RATIO_CHILD,
        POW((a.conversion_new_ratio - b.CNR), 2) AS CONVERSION_NEW_RATIO_Y,
        -- 老客轉化率
        (a.revenue_num - b.R) * (a.conversion_old_ratio - b.COR)     AS CONVERSION_OLD_RATIO_CHILD,
        POW((a.conversion_old_ratio - b.COR), 2) AS CONVERSION_OLD_RATIO_Y
    FROM pri_result.migo_kpi a LEFT OUTER JOIN (
        SELECT seller_id, cate_id,
            AVG(revenue_num) AS R,              -- 平均營業額
            AVG(member_num) AS AM,              -- 有效客戶數的平均數
            AVG(member_new_num) AS AAN,         -- 新增客戶數
            AVG(member_sleep_num) AS ASN,       -- 流失客戶數
            AVG(member_new_ratio) AS ANR,       -- 客戶新增率的的平均數
            AVG(member_sleep_ratio) AS ASR,     -- 客戶流失率的平均數

            AVG(active_ratio) AS AR,            -- 整體客戶活躍度
            AVG(active_old_ratio) AS AOR,       -- 老客戶活躍度
            -- 尚未確定（活躍老客戶數）
            AVG(active_old_num) AS AON,         -- 老客戶活躍數

            AVG(stickiness_ratio) AS SR,        -- 黏著度
            AVG(stickiness_new_ratio) AS SNR,   -- 新客黏著度
            AVG(stickiness_old_ratio) AS SOR,   -- 老客黏著度

            AVG(arpu_num) AS AN,                -- 有效客單價
            AVG(arpu_new_num) AS ANN,           -- 首購客單價
            AVG(arpu_old_num) AS AOON,          -- 重複客單價

            AVG(visitor_num) AS VN,             -- 所有客戶拜訪數
            AVG(visitor_old_num) AS VON,        -- 已成交訪客拜訪次數
            AVG(visitor_new_num) AS VNN,        -- 未成交客戶拜訪次數

            AVG(conversion_ratio) AS CR,        -- 整體轉換率
            AVG(conversion_new_ratio) AS CNR,   -- 首購轉化率
            AVG(conversion_old_ratio) AS COR    -- 老客轉化率
        FROM pri_result.migo_kpi
        WHERE dt >= to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd') AND dt <= '${date_ymd}'
        GROUP BY seller_id, cate_id
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) c ON a.cate_id = c.cate_level1_id
) a
GROUP BY a.seller_id, a.cate_id, a.cate_level1_name;

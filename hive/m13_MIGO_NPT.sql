INSERT OVERWRITE TABLE pri_result.migo_npt_ranking_730 PARTITION (dt="${date_ymd}")

SELECT seller_id, buyer_id, cate_id, npt_id, hazard_rate,
    RANK() over(PARTITION BY seller_id, cate_id, npt_id ORDER BY hazard_rate DESC) AS hazard_ranking
FROM (
    SELECT seller_id, buyer_id, cate_id, npt_id,
        CASE CDF WHEN 1.0 THEN 0.0 ELSE PDF / (1-CDF) END * 10000 AS hazard_rate
    FROM (
        SELECT seller_id, buyer_id, cate_id, npt_id,
                SQRT(2) / (sigma_adj * SQRT(3.1415926)) * EXP( -1 * POW(npt, 2)/(2* POW(sigma_adj, 2))) AS PDF,
                2 * MIGO_UDF_CDF(CAST(npt AS double)) / (sigma_adj * SQRT(2)) AS CDF
        FROM (
            SELECT  a.seller_id,
                    a.buyer_id,
                    a.cate_id,
                    a.adj_gap_mean * SQRT(3.1415926/2) AS sigma_adj,
                    b.npt, b.npt_id
            FROM pri_result.migo_kpi_1_adj_gap_mean a JOIN (
                SELECT seller_id, buyer_id, cate_id,
                        DATEDIFF(DATEADD(TO_DATE("${date_ymd}", "yyyymmdd"), 30, "dd"),
                                TO_DATE(MAX(alipay_time), "yyyy-mm-dd"),
                                "dd") AS npt, 'NPT30' AS npt_id
                FROM pri_result.migo_kpi_1_individual_pi
                WHERE purchasing_interval IS NOT NULL AND dt = '${date_ymd}'
                GROUP BY seller_id, buyer_id, cate_id

                UNION ALL

                SELECT seller_id, buyer_id, cate_id,
                        DATEDIFF(DATEADD(TO_DATE("${date_ymd}", "yyyymmdd"), 7, "dd"),
                                TO_DATE(MAX(alipay_time), "yyyy-mm-dd"),
                                "dd") AS npt, 'NPT7' AS npt_id
                FROM pri_result.migo_kpi_1_individual_pi
                WHERE purchasing_interval IS NOT NULL AND dt = '${date_ymd}'
                GROUP BY seller_id, buyer_id, cate_id
            ) b ON a.seller_id = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id WHERE a.dt = '${date_ymd}'
        ) a
    ) a
) a;

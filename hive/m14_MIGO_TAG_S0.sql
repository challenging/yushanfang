INSERT OVERWRITE TABLE pri_result.migo_tagging_s0 PARTITION (dt='${date_ymd}')

--Stickiness
--- S0 （2Group_Mean >= (now date – last purchasing date) >= 1 Group_Mean）
SELECT DISTINCT f.seller_id , f.buyer_id, f.cate_id, 1301 AS tagging
FROM (
    SELECT a.seller_id, a.buyer_id, a.cate_id
    , datediff(to_date('${date_ymd}', 'yyyymmdd'), to_date(a.alipay_time ,'yyyy-mm-dd'), 'dd') AS last_p_interval
    FROM pri_result.migo_kpi_1_individual_pi a
    JOIN (
        SELECT seller_id, buyer_id, cate_id, MAX(alipay_time_seq) as max_seq
        FROM pri_result.migo_kpi_1_individual_pi p
        WHERE dt = '${date_ymd}'
        GROUP BY seller_id, buyer_id, cate_id
    ) b ON a.seller_id  = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id AND a.alipay_time_seq = b.max_seq
) f JOIN (
   SELECT seller_id, buyer_id, cate_id, g_gap_mean
   FROM pri_result.migo_kpi_1_adj_gap_mean
   WHERE dt = '${date_ymd}'
) c ON f.seller_id = c.seller_id AND f.buyer_id = c.buyer_id AND f.cate_id = c.cate_id
WHERE last_p_interval >= g_gap_mean AND last_p_interval <= (2 * g_gap_mean);

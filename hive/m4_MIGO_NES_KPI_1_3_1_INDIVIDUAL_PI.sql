INSERT OVERWRITE TABLE pri_result.migo_kpi_1_adj_gap_mean PARTITION (dt='${date_ymd}')

SELECT seller_id, buyer_id, cate_id, p_rep_count ,p_w1 ,p_w2
, CASE WHEN (alpha = -999 or theta = -999) THEN g_gap_mean
       WHEN (p_rep_count = 0) THEN  k/((alpha-1)*theta)
       WHEN (p_rep_count > 0) THEN  k*(p_w1*p_gap_mean + p_w2/theta) END AS p_adj_gap_mean
, k, alpha, theta, g_gap_mean
FROM (
    SELECT a.seller_id, a.buyer_id, a.cate_id, a.p_rep_count, b.k, b.alpha, b.theta, a.p_gap_mean
    , p_rep_count / ((k*p_rep_count)+alpha-1) AS p_w1
    , 1/((k*p_rep_count) + alpha-1) as p_w2
    , g_gap_mean
    FROM (
        SELECT seller_id, buyer_id, cate_id, COUNT(*)-1 AS p_rep_count, AVG(purchasing_interval) as p_gap_mean
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}'
        GROUP BY seller_id, cate_id, buyer_id
    ) a JOIN (
        SELECT seller_id , cate_id, alpha ,theta, k
        FROM pri_result.migo_kpi_1_group_arg
        WHERE dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id JOIN (
        SELECT seller_id, cate_id, AVG(purchasing_interval) as g_gap_mean
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}'
        GROUP BY seller_id, cate_id
    ) c ON a.seller_id = c.seller_id AND a.cate_id = c.cate_id
) d
;


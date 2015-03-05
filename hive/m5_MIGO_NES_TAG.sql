INSERT OVERWRITE TABLE pri_result.migo_tagging_nes PARTITION (dt='${date_ymd}')

SELECT c.seller_id, c.buyer_id, c.cate_id --, c.last_days, adj_gap_mean, c.alipayCount
, CASE WHEN (c.last_days > (2.9154 * adj_gap_mean)) THEN 'S3'
       WHEN (c.last_days > (2.5515 * adj_gap_mean)) THEN 'S2'
       WHEN (c.last_days > (2.0389 * adj_gap_mean)) THEN 'S1'
       WHEN ((c.first_days < 30) and (c.alipayCount = 1))  THEN 'N0'
       WHEN ((c.first_days < 30) and
             (datediff(to_date(c.second_alipay_time,'yyyy-mm-dd'), to_date(c.first_alipay_time,'yyyy-mm-dd'), 'dd')) < adj_gap_mean) THEN 'EB'
       ELSE 'E0' END AS NES_Tag
FROM (
    --get last interval
    SELECT a.seller_id ,a.buyer_id , a.cate_id, adj_gap_mean, c.last_days ,c.first_days
         , c.first_alipay_time ,d.second_alipay_time, c.alipaycount
    FROM (
        SELECT  seller_id ,buyer_id , cate_id, adj_gap_mean
        FROM pri_result.migo_kpi_1_adj_gap_mean
        WHERE dt = '${date_ymd}'
    ) a JOIN (
        --get first days , last days
        SELECT seller_id, buyer_id, cate_id, first_alipay_time
               ,datediff(to_date('${date_ymd}', 'yyyymmdd'), to_date(first_alipay_time,'yyyy-mm-dd'), 'dd') AS first_days
               ,datediff(to_date('${date_ymd}', 'yyyymmdd'), to_date(last_alipay_time ,'yyyy-mm-dd'), 'dd') AS last_days
               ,alipayCount
        FROM (
            SELECT seller_id, buyer_id, cate_id, min(alipay_time) as first_alipay_time, max(alipay_time) AS last_alipay_time, COUNT(*) as alipayCount
            FROM pri_result.migo_kpi_1_individual_pi
            WHERE dt = '${date_ymd}'
            GROUP BY seller_id, buyer_id, cate_id
        ) a
    ) c ON a.seller_id = c.seller_id AND a.buyer_id = c.buyer_id AND a.cate_id = c.cate_id LEFT OUTER JOIN (
        --second alipay_time for early bird
        SELECT seller_id, buyer_id, cate_id, alipay_time AS second_alipay_time
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}' AND alipay_time_seq = 2
    ) d ON a.seller_id = d.seller_id AND a.buyer_id = d.buyer_id AND a.cate_id = d.cate_id
) c ;

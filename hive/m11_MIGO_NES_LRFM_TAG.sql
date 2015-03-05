INSERT OVERWRITE TABLE pri_result.migo_lrfm_results PARTITION (dt='${date_ymd}')

SELECT b.seller_id, b.buyer_id, b.cate_id,
CASE WHEN ((b.rank_lifetime / a.ML) <= 0.25) THEN "LH"
     WHEN ((b.rank_lifetime / a.ML) <= 0.50) THEN "LM"
     ELSE "LL" END ,
CASE WHEN ((b.rank_recency / a.MR) <= 0.25) THEN "RH"
     WHEN ((b.rank_recency / a.MR) <= 0.50) THEN "RM"
     ELSE "RL" END ,
CASE WHEN ((b.rank_frequency / a.MF) <= 0.25) THEN "FH"
     WHEN ((b.rank_frequency / a.MF) <= 0.50) THEN "FM"
     ELSE "FL" END ,
CASE WHEN ((b.rank_monetary / a.MM) <= 0.25) THEN "MH"
     WHEN ((b.rank_monetary / a.MM) <= 0.50) THEN "MM"
     ELSE "ML" END
FROM
    (  SELECT seller_id, cate_id, MAX(rank_lifetime) as ML, MAX(rank_recency) as MR, MAX(rank_frequency) as MF, MAX(rank_monetary) as MM
        FROM pri_result.migo_lrfm_1
        WHERE dt = '${date_ymd}'
        GROUP BY seller_id, cate_id
    ) a JOIN (
        SELECT seller_id, buyer_id, cate_id, rank_lifetime, rank_recency, rank_frequency, rank_monetary FROM pri_result.migo_lrfm_1
        WHERE dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id
;

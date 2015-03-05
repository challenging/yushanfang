INSERT OVERWRITE TABLE pri_result.migo_cf_for_mysql PARTITION (dt='${date_ymd}')

SELECT '${date_ymd}' as thedate, seller_id, tagging_id, key2 AS auction_id, AVG(rank_num) as avg_rank_num
FROM  pri_result.migo_CF_similar_martix_9 
WHERE dt = '${date_ymd}'
GROUP BY seller_id, tagging_id, key2


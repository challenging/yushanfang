INSERT OVERWRITE TABLE pri_result.migo_dmp_arpu PARTITION (dt='${date_ymd}')

SELECT '${date_ymd}', seller_id, buyer_id, tagging_id
FROM  pri_result.migo_tagging 
WHERE dt = '${date_ymd}' AND cate_id = -1 AND tagging_id IN (1400, 1401)

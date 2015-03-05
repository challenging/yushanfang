-- migo_dmp_stickiness 1300
INSERT OVERWRITE TABLE pri_result.migo_dmp_stickiness PARTITION  (dt='${date_ymd}')

SELECT  '${date_ymd}' AS thedate,  
        seller_id,  
        buyer_id, 
        tagging_id
FROM    pri_result.migo_tagging
WHERE   dt = '${date_ymd}'
    AND cate_id = -1 
    AND tagging_id IN (1300)


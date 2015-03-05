INSERT OVERWRITE TABLE pri_result.migo_dmp_tag_l_hml PARTITION (dt='${date_ymd}')
---LH:2200,LM:2201,LL:2202
SELECT  thedate, 
        seller_id, 
        buyer_id, 
        tagging_id
FROM
(
    SELECT  '${date_ymd}' AS thedate, 
            seller_id, 
            buyer_id, 
            CASE WHEN L = "LH" THEN 2200
                 WHEN L = "LM" THEN 2201
                 ELSE 2202 END AS tagging_id
    FROM    pri_result.migo_lrfm_results
    WHERE   dt = '${date_ymd}' AND cate_id = -1
)z
